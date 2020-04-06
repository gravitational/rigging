package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log/syslog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/gravitational/rigging"

	yaml "github.com/ghodss/yaml"
	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
	logrusSyslog "github.com/sirupsen/logrus/hooks/syslog"
	"gopkg.in/alecthomas/kingpin.v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var quiet bool
	if err := run(&quiet); err != nil {
		log.Error(trace.DebugReport(err))
		if !quiet {
			fmt.Printf("ERROR: %v\n", err.Error())
		}
		os.Exit(255)
	}
}

func run(quiet *bool) error {
	var (
		app = kingpin.New("rig", "CLI utility to simplify K8s updates")

		debug      = app.Flag("debug", "turn on debug logging").Bool()
		kubeConfig = app.Flag("kubeconfig", "path to kubeconfig").Default(filepath.Join(os.Getenv("HOME"), ".kube", "config")).String()
		namespace  = app.Flag("namespace", "Namespace of the changesets").Default(rigging.DefaultNamespace).String()

		cupsert          = app.Command("upsert", "Upsert resources in the context of a changeset")
		cupsertChangeset = Ref(cupsert.Flag("changeset", "name of the changeset").Short('c').Envar(changesetEnvVar).Required())
		cupsertFile      = cupsert.Flag("file", "file with new resource spec").Short('f').Required().String()
		cupsertForce     = cupsert.Flag("force", "Ignore error if resource is already updated").Bool()

		cupsertConfigMap          = app.Command("configmap", "Upsert configmap in the context of a changeset")
		cupsertConfigMapChangeset = Ref(cupsertConfigMap.Flag("changeset", "name of the changeset").Short('c').Envar(changesetEnvVar).Required())
		cupsertConfigMapName      = cupsertConfigMap.Arg("name", "ConfigMap name").Required().String()
		cupsertConfigMapNamespace = cupsertConfigMap.Flag("resource-namespace", "ConfigMap namespace").Default(rigging.DefaultNamespace).String()
		cupsertConfigMapFiles     = cupsertConfigMap.Flag("from-file", "files or directories with contents").Strings()
		cupsertConfigMapLiterals  = cupsertConfigMap.Flag("from-literal", "literals in form of key=val").Strings()
		cupsertConfigMapForce     = cupsertConfigMap.Flag("force", "Ignore error if resource is already updated").Bool()

		cstatus         = app.Command("status", "Check status of all operations in a changeset")
		cstatusResource = Ref(cstatus.Arg("resource", "resource to check, e.g. tx/tx1").Required())
		cstatusAttempts = cstatus.Flag("retry-attempts", "Number of attempts to check status").Default("1").Int()
		cstatusPeriod   = cstatus.Flag("retry-period", "Time between status checks").Default(fmt.Sprintf("%v", rigging.DefaultRetryPeriod)).Duration()

		cget          = app.Command("get", "Display one or many changesets")
		cgetChangeset = Ref(cget.Flag("changeset", "Changeset name").Short('c').Envar(changesetEnvVar))
		cgetOut       = cget.Flag("output", "output type, one of 'yaml' or 'text'").Short('o').Default("").String()

		ctr = app.Command("cs", "low level operations on changesets")

		ctrDelete          = ctr.Command("delete", "Delete a changeset by name")
		ctrDeleteForce     = ctrDelete.Flag("force", "Ignore error if resource is not found").Bool()
		ctrDeleteChangeset = Ref(ctrDelete.Flag("changeset", "Changeset name").Short('c').Envar(changesetEnvVar).Required())

		crevert          = app.Command("revert", "Revert the changeset")
		crevertChangeset = Ref(crevert.Flag("changeset", "name of the changeset").Short('c').Envar(changesetEnvVar).Required())
		crevertUID       = crevert.Flag("uid", "ID of the specific operation to revert").String()

		cfreeze          = app.Command("freeze", "Freeze the changeset")
		cfreezeChangeset = Ref(cfreeze.Flag("changeset", "name of the changeset").Short('c').Envar(changesetEnvVar).Required())

		cdelete                  = app.Command("delete", "Delete a resource in a context of a changeset")
		cdeleteForce             = cdelete.Flag("force", "Ignore error if resource is not found").Bool()
		cdeleteCascade           = cdelete.Flag("cascade", "Delete sub resouces, e.g. Pods for Daemonset").Default("true").Bool()
		cdeleteChangeset         = Ref(cdelete.Flag("changeset", "Changeset name").Short('c').Envar(changesetEnvVar).Required())
		cdeleteResource          = Ref(cdelete.Arg("resource", "Resource name to delete").Required())
		cdeleteResourceNamespace = cdelete.Flag("resource-namespace", "Resource namespace").Default(rigging.DefaultNamespace).String()
	)
	app.Flag("quiet", "Suppress program output").Short('q').BoolVar(quiet)

	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		return trace.Wrap(err)
	}

	switch {
	case *quiet:
		TurnOffLogging()
	case *debug:
		InitLoggerDebug()
	default:
		InitLoggerCLI()
	}

	client, config, err := getClient(*kubeConfig)
	if err != nil {
		return trace.Wrap(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		exitSignals := make(chan os.Signal, 1)
		signal.Notify(exitSignals, syscall.SIGTERM, syscall.SIGINT)

		select {
		case sig := <-exitSignals:
			log.Infof("signal: %v", sig)
			cancel()
		}
	}()

	switch cmd {
	case cupsert.FullCommand():
		return upsert(ctx, client, config, *namespace, *cupsertChangeset, *cupsertFile, *cupsertForce)
	case cstatus.FullCommand():
		return status(ctx, client, config, *namespace, *cstatusResource, *cstatusAttempts, *cstatusPeriod)
	case cget.FullCommand():
		return get(ctx, client, config, *namespace, *cgetChangeset, *cgetOut)
	case cdelete.FullCommand():
		return deleteResource(ctx, client, config, *namespace, *cdeleteChangeset, *cdeleteResourceNamespace, *cdeleteResource, *cdeleteCascade, *cdeleteForce)
	case ctrDelete.FullCommand():
		return csDelete(ctx, client, config, *namespace, *ctrDeleteChangeset, *ctrDeleteForce)
	case crevert.FullCommand():
		return revert(ctx, client, config, *namespace, *crevertChangeset, *crevertUID)
	case cfreeze.FullCommand():
		return freeze(ctx, client, config, *namespace, *cfreezeChangeset)
	case cupsertConfigMap.FullCommand():
		return upsertConfigMap(ctx, client, config, *namespace, *cupsertConfigMapChangeset, *cupsertConfigMapName, *cupsertConfigMapNamespace, *cupsertConfigMapFiles, *cupsertConfigMapLiterals, *cupsertConfigMapForce)
	}

	return trace.BadParameter("unsupported command: %v", cmd)
}

func getClient(configPath string) (*kubernetes.Clientset, *rest.Config, error) {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err == nil {
		client, err := kubernetes.NewForConfig(config)
		if err != nil {
			return nil, nil, trace.Wrap(err)
		}
		return client, config, nil
	}

	config, err = clientcmd.BuildConfigFromFlags("", configPath)
	if err != nil {
		return nil, nil, trace.Wrap(err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, trace.Wrap(err)
	}
	return client, config, nil
}

func Ref(s kingpin.Settings) *rigging.Ref {
	r := new(rigging.Ref)
	s.SetValue(r)
	return r
}

func revert(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, namespace string, changeset rigging.Ref, uid string) error {
	if changeset.Kind != rigging.KindChangeset {
		return trace.BadParameter("expected %v, got %v", rigging.KindChangeset, changeset.Kind)
	}
	cs, err := rigging.NewChangeset(ctx, rigging.ChangesetConfig{
		Client:    client,
		Config:    config,
		Name:      changeset.Name,
		Namespace: namespace,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	err = cs.Revert(ctx, uid)
	if err != nil {
		return trace.Wrap(err)
	}
	if uid != "" {
		fmt.Printf("Operation %v in changeset %v reverted.\n", uid, changeset.Name)
		return nil
	}
	fmt.Printf("Changeset %v reverted.\n", changeset.Name)
	return nil
}

func freeze(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, namespace string, changeset rigging.Ref) error {
	if changeset.Kind != rigging.KindChangeset {
		return trace.BadParameter("expected %v, got %v", rigging.KindChangeset, changeset.Kind)
	}
	cs, err := rigging.NewChangeset(ctx, rigging.ChangesetConfig{
		Client:    client,
		Config:    config,
		Name:      changeset.Name,
		Namespace: namespace,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	err = cs.Freeze(ctx)
	if err != nil {
		return trace.Wrap(err)
	}
	fmt.Printf("changeset %v frozen, no further modifications are allowed\n", changeset.Name)
	return nil
}

func deleteResource(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, namespace string, changeset rigging.Ref, resourceNamespace string, resource rigging.Ref, cascade, force bool) error {
	if changeset.Kind != rigging.KindChangeset {
		return trace.BadParameter("expected %v, got %v", rigging.KindChangeset, changeset.Kind)
	}
	cs, err := rigging.NewChangeset(ctx, rigging.ChangesetConfig{
		Client:    client,
		Config:    config,
		Name:      changeset.Name,
		Namespace: namespace,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	err = cs.DeleteResource(ctx, resourceNamespace, resource, cascade)
	if err != nil {
		if force && trace.IsNotFound(err) {
			fmt.Printf("%v is not found, force flag is set, %v not updated, ignoring \n", resource.String(), changeset.Name)
			return nil
		}
		return trace.Wrap(err)
	}
	fmt.Printf("changeset %v updated \n", changeset.Name)
	return nil
}

func upsertConfigMap(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, changesetNamespace string, changeset rigging.Ref, configMapName, configMapNamespace string, files []string, literals []string, force bool) error {
	if changeset.Kind != rigging.KindChangeset {
		return trace.BadParameter("expected %v, got %v", rigging.KindChangeset, changeset.Kind)
	}
	configMap, err := rigging.GenerateConfigMap(configMapName, configMapNamespace, files, literals)
	if err != nil {
		return trace.Wrap(err)
	}
	cs, err := rigging.NewChangeset(ctx, rigging.ChangesetConfig{
		Client:    client,
		Config:    config,
		Name:      changeset.Name,
		Namespace: changesetNamespace,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	data, err := yaml.Marshal(configMap)
	if err != nil {
		return trace.Wrap(err)
	}

	err = cs.Upsert(ctx, data, force)
	if err != nil {
		return trace.Wrap(err)
	}
	fmt.Printf("changeset %v updated \n", changeset.Name)
	return nil
}

func upsert(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, namespace string, changeset rigging.Ref, filePath string, force bool) error {
	if changeset.Kind != rigging.KindChangeset {
		return trace.BadParameter("expected %v, got %v", rigging.KindChangeset, changeset.Kind)
	}
	data, err := ReadPath(filePath)
	if err != nil {
		return trace.Wrap(err)
	}
	cs, err := rigging.NewChangeset(ctx, rigging.ChangesetConfig{
		Client:    client,
		Config:    config,
		Name:      changeset.Name,
		Namespace: namespace,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	err = cs.Upsert(ctx, data, force)
	if err != nil {
		return trace.Wrap(err)
	}
	fmt.Printf("changeset %v updated \n", changeset.Name)
	return nil
}

func status(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, namespace string, resource rigging.Ref,
	retryAttempts int, retryPeriod time.Duration) error {
	switch resource.Kind {
	case rigging.KindChangeset:
		cs, err := rigging.NewChangeset(ctx, rigging.ChangesetConfig{
			Client:    client,
			Config:    config,
			Name:      resource.Name,
			Namespace: namespace,
		})
		if err != nil {
			return trace.Wrap(err)
		}
		err = cs.Status(ctx, "", retryAttempts, retryPeriod)
		if err != nil {
			return trace.Wrap(err)
		}
		fmt.Printf("no errors detected for %v\n", resource.Name)
		return nil
	case rigging.KindDaemonSet:
		daemonSet, err := client.Apps().DaemonSets(namespace).Get(resource.Name, metav1.GetOptions{})
		if err != nil {
			return trace.Wrap(err)
		}
		updater, err := rigging.NewDaemonSetControl(rigging.DSConfig{
			DaemonSet: daemonSet,
			Client:    client,
		})
		if err != nil {
			return trace.Wrap(err)
		}
		return rigging.PollStatus(ctx, retryAttempts, retryPeriod, updater)
	case rigging.KindDeployment:
		deployment, err := client.Apps().Deployments(namespace).Get(resource.Name, metav1.GetOptions{})
		if err != nil {
			return trace.Wrap(err)
		}
		updater, err := rigging.NewDeploymentControl(rigging.DeploymentConfig{
			Deployment: deployment,
			Client:     client,
		})
		if err != nil {
			return trace.Wrap(err)
		}
		return rigging.PollStatus(ctx, retryAttempts, retryPeriod, updater)
	}
	return trace.BadParameter("don't know how to check status of %v", resource.Kind)
}

const (
	outputYAML = "yaml"
	// humanDateFormat is a human readable date formatting
	humanDateFormat = "Mon Jan _2 15:04 UTC"
	changesetEnvVar = "RIG_CHANGESET"
)

func get(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, namespace string, ref rigging.Ref, output string) error {
	cs, err := rigging.NewChangeset(ctx, rigging.ChangesetConfig{
		Client:    client,
		Config:    config,
		Name:      ref.Name,
		Namespace: namespace,
	})
	if err != nil {
		return trace.Wrap(err)
	}

	if ref.Name == "" {
		changesets, err := cs.List(ctx)
		if err != nil {
			return trace.Wrap(err)
		}
		switch output {
		case outputYAML:
			data, err := yaml.Marshal(changesets)
			if err != nil {
				return trace.Wrap(err)
			}
			fmt.Printf("%v\n", string(data))
			return nil
		default:
			if len(changesets.Items) == 0 {
				fmt.Printf("No changesets found\n")
				return nil
			}
			w := new(tabwriter.Writer)
			w.Init(os.Stdout, 0, 8, 1, '\t', 0)
			defer w.Flush()
			fmt.Fprintf(w, "Name\tCreated\tStatus\tOperations\n")
			for _, tr := range changesets.Items {
				fmt.Fprintf(w, "%v\t%v\t%v\t%v\n", tr.Name, tr.CreationTimestamp.Format(humanDateFormat), tr.Spec.Status, len(tr.Spec.Items))
			}
			return nil
		}
	}
	tr, err := cs.Get(ctx, namespace, ref.Name)
	if err != nil {
		return trace.Wrap(err)
	}
	switch output {
	case outputYAML:
		data, err := yaml.Marshal(tr)
		if err != nil {
			return trace.Wrap(err)
		}
		fmt.Printf("%v\n", string(data))
		return nil
	default:
		fmt.Printf("Changeset %v in namespace %v\n\n", tr.Name, tr.Namespace)
		w := new(tabwriter.Writer)
		w.Init(os.Stdout, 0, 8, 1, '\t', 0)
		defer w.Flush()
		fmt.Fprintf(w, "Operation\tTime\tStatus\tDescription\n")
		for i, op := range tr.Spec.Items {
			var info string
			opInfo, err := rigging.GetOperationInfo(op)
			if err != nil {
				info = err.Error()
			} else {
				info = opInfo.String()
			}
			fmt.Fprintf(w, "%v\t%v\t%v\t%v\n", i, op.CreationTimestamp.Format(humanDateFormat), op.Status, info)
		}
		return nil
	}
}

func csDelete(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, namespace string, tr rigging.Ref, force bool) error {
	cs, err := rigging.NewChangeset(ctx, rigging.ChangesetConfig{
		Client:    client,
		Config:    config,
		Name:      tr.Name,
		Namespace: namespace,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	err = cs.Delete(ctx)
	if err != nil {
		if trace.IsNotFound(err) && force {
			fmt.Printf("%v is not found and force is set\n", tr.Name)
			return nil
		}
		return trace.Wrap(err)
	}
	fmt.Printf("%v has been deleted\n", tr.Name)
	return nil
}

// InitLoggerCLI tools by default log into syslog, not stderr
func InitLoggerCLI() {
	log.SetLevel(log.WarnLevel)
	// clear existing hooks:
	log.StandardLogger().Hooks = make(log.LevelHooks)
	log.SetFormatter(&trace.TextFormatter{})

	hook, err := logrusSyslog.NewSyslogHook("", "", syslog.LOG_WARNING, "")
	if err != nil {
		// Bail out if syslog is not available
		return
	}
	log.AddHook(hook)
	log.SetOutput(ioutil.Discard)
}

// InitLoggerDebug configures the logger to dump everything to stderr
func InitLoggerDebug() {
	// clear existing hooks:
	log.StandardLogger().Hooks = make(log.LevelHooks)
	log.SetFormatter(&trace.TextFormatter{})
	log.SetOutput(os.Stderr)
	log.SetLevel(log.DebugLevel)
}

// TurnOffLogging disable logging
func TurnOffLogging() {
	log.StandardLogger().Hooks = make(log.LevelHooks)
	log.SetOutput(ioutil.Discard)
	log.SetLevel(log.FatalLevel)
}

// NormalizePath normalises path, evaluating symlinks and converting local
// paths to absolute
func NormalizePath(path string) (string, error) {
	s, err := filepath.Abs(path)
	if err != nil {
		return "", trace.ConvertSystemError(err)
	}
	abs, err := filepath.EvalSymlinks(s)
	if err != nil {
		return "", trace.ConvertSystemError(err)
	}
	return abs, nil
}

// ReadPath reads file at given path
func ReadPath(path string) ([]byte, error) {
	abs, err := NormalizePath(path)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	bytes, err := ioutil.ReadFile(abs)
	if err != nil {
		return nil, trace.ConvertSystemError(err)
	}
	return bytes, nil
}
