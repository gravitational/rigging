package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log/syslog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/gravitational/rigging"

	log "github.com/Sirupsen/logrus"
	logrusSyslog "github.com/Sirupsen/logrus/hooks/syslog"
	yaml "github.com/ghodss/yaml"
	"github.com/gravitational/trace"
	"gopkg.in/alecthomas/kingpin.v2"
	"k8s.io/client-go/1.4/kubernetes"
	"k8s.io/client-go/1.4/rest"
	"k8s.io/client-go/1.4/tools/clientcmd"
)

func main() {
	InitLoggerCLI()
	if err := run(); err != nil {
		log.Error(trace.DebugReport(err))
		fmt.Printf("ERROR: %v\n", err.Error())
		os.Exit(255)
	}
}

func run() error {
	var (
		app = kingpin.New("rig", "CLI utility to simplify K8s updates")

		debug      = app.Flag("debug", "turn on debug logging").Bool()
		kubeConfig = app.Flag("kubeconfig", "path to kubeconfig").Default(filepath.Join(os.Getenv("HOME"), ".kube", "config")).String()
		namespace  = app.Flag("namespace", "Namespace of the transactions").Default(rigging.DefaultNamespace).String()

		cupsert            = app.Command("upsert", "Upsert resources in the context of a transaction")
		cupsertTransaction = Ref(cupsert.Arg("transaction", "name of the transaction").Required())
		cupsertFile        = cupsert.Flag("file", "file with new resource spec").Short('f').Required().String()

		cstatus         = app.Command("status", "Check status of all operations in a transaction")
		cstatusResource = Ref(cstatus.Arg("resource", "resource to check, e.g. tx/tx1").Required())
		cstatusAttempts = cstatus.Flag("retry-attempts", "file with new daemon set spec").Default("1").Int()
		cstatusPeriod   = cstatus.Flag("retry-period", "file with new daemon set spec").Default(fmt.Sprintf("%v", rigging.DefaultRetryPeriod)).Duration()

		cget            = app.Command("get", "Display one or many transactions")
		cgetTransaction = Ref(cget.Arg("transaction", "Transaction name"))
		cgetOut         = cget.Flag("output", "output type, one of 'yaml' or 'text'").Short('o').Default("").String()

		ctr = app.Command("tx", "low level operations on transactions")

		ctrDelete            = ctr.Command("delete", "Delete a transaction by name")
		ctrDeleteTransaction = Ref(ctrDelete.Arg("transaction", "Transaction name").Required())

		crollback            = app.Command("rollback", "Rollback the transaction")
		crollbackTransaction = Ref(crollback.Arg("transaction", "name of the transaction").Required())

		ccommit            = app.Command("commit", "Commit the transaction")
		ccommitTransaction = Ref(ccommit.Arg("transaction", "name of the transaction").Required())

		cdelete                  = app.Command("delete", "Delete a resource in a context of a transaction")
		cdeleteCascade           = cdelete.Flag("cascade", "Delete sub resouces, e.g. Pods for Daemonset").Default("true").Bool()
		cdeleteTransaction       = Ref(cdelete.Arg("transaction", "Transaction name").Required())
		cdeleteResource          = Ref(cdelete.Arg("resource", "Resource name to delete").Required())
		cdeleteResourceNamespace = cdelete.Flag("resource-namespace", "Resource name to delete").Default(rigging.DefaultNamespace).String()
	)

	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		return trace.Wrap(err)
	}

	if *debug {
		InitLoggerDebug()
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
		return upsert(ctx, client, config, *namespace, *cupsertTransaction, *cupsertFile)
	case cstatus.FullCommand():
		return status(ctx, client, config, *namespace, *cstatusResource, *cstatusAttempts, *cstatusPeriod)
	case cget.FullCommand():
		return get(ctx, client, config, *namespace, *cgetTransaction, *cgetOut)
	case cdelete.FullCommand():
		return deleteResource(ctx, client, config, *namespace, *cdeleteTransaction, *cdeleteResourceNamespace, *cdeleteResource, *cdeleteCascade)
	case ctrDelete.FullCommand():
		return txDelete(ctx, client, config, *namespace, *ctrDeleteTransaction)
	case crollback.FullCommand():
		return rollback(ctx, client, config, *namespace, *crollbackTransaction)
	case ccommit.FullCommand():
		return commit(ctx, client, config, *namespace, *ccommitTransaction)
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

func rollback(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, namespace string, transaction rigging.Ref) error {
	if transaction.Kind != rigging.KindTransaction {
		return trace.BadParameter("expected %v, got %v", rigging.KindTransaction, transaction.Kind)
	}
	tx, err := rigging.NewTransaction(rigging.TransactionConfig{
		Client: client,
		Config: config,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	err = tx.Rollback(ctx, namespace, transaction.Name)
	if err != nil {
		return trace.Wrap(err)
	}
	fmt.Printf("transaction %v rolled back \n", transaction.Name)
	return nil
}

func commit(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, namespace string, transaction rigging.Ref) error {
	if transaction.Kind != rigging.KindTransaction {
		return trace.BadParameter("expected %v, got %v", rigging.KindTransaction, transaction.Kind)
	}
	tx, err := rigging.NewTransaction(rigging.TransactionConfig{
		Client: client,
		Config: config,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	err = tx.Commit(ctx, namespace, transaction.Name)
	if err != nil {
		return trace.Wrap(err)
	}
	fmt.Printf("transaction %v commited, no further modifications are allowed \n", transaction.Name)
	return nil
}

func deleteResource(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, namespace string, transaction rigging.Ref, resourceNamespace string, resource rigging.Ref, cascade bool) error {
	if transaction.Kind != rigging.KindTransaction {
		return trace.BadParameter("expected %v, got %v", rigging.KindTransaction, transaction.Kind)
	}
	tx, err := rigging.NewTransaction(rigging.TransactionConfig{
		Client: client,
		Config: config,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	err = tx.DeleteResource(ctx, namespace, transaction.Name, resourceNamespace, resource, cascade)
	if err != nil {
		return trace.Wrap(err)
	}
	fmt.Printf("transaction %v updated \n", transaction.Name)
	return nil
}

func upsert(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, namespace string, transaction rigging.Ref, filePath string) error {
	if transaction.Kind != rigging.KindTransaction {
		return trace.BadParameter("expected %v, got %v", rigging.KindTransaction, transaction.Kind)
	}
	data, err := ReadPath(filePath)
	if err != nil {
		return trace.Wrap(err)
	}
	tx, err := rigging.NewTransaction(rigging.TransactionConfig{
		Client: client,
		Config: config,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	err = tx.Upsert(ctx, namespace, transaction.Name, data)
	if err != nil {
		return trace.Wrap(err)
	}
	fmt.Printf("transaction %v updated \n", transaction.Name)
	return nil
}

func status(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, namespace string, resource rigging.Ref, retryAttempts int, retryPeriod time.Duration) error {
	switch resource.Kind {
	case rigging.KindTransaction:
		tx, err := rigging.NewTransaction(rigging.TransactionConfig{
			Client: client,
			Config: config,
		})
		if err != nil {
			return trace.Wrap(err)
		}
		err = tx.Status(ctx, namespace, resource.Name, retryAttempts, retryPeriod)
		if err != nil {
			return trace.Wrap(err)
		}
		fmt.Printf("no errors detected for %v\n", resource.Name)
		return nil
	case rigging.KindDaemonSet:
		ds, err := client.Extensions().DaemonSets(namespace).Get(resource.Name)
		if err != nil {
			return trace.Wrap(err)
		}
		updater, err := rigging.NewDSControl(rigging.DSConfig{
			DaemonSet: ds,
			Client:    client,
		})
		if err != nil {
			return trace.Wrap(err)
		}
		return updater.Status(ctx, retryAttempts, retryPeriod)
	}
	return trace.BadParameter("don't know how to check status of %v", resource.Kind)
}

const (
	outputYAML = "yaml"
	outputText = "text"
	outputJSON = "json"
)

func get(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, namespace string, ref rigging.Ref, output string) error {
	tx, err := rigging.NewTransaction(rigging.TransactionConfig{
		Client: client,
		Config: config,
	})
	if err != nil {
		return trace.Wrap(err)
	}

	if ref.Name == "" {
		transactions, err := tx.List(ctx, namespace)
		if err != nil {
			return trace.Wrap(err)
		}
		switch output {
		case outputYAML:
			data, err := yaml.Marshal(transactions)
			if err != nil {
				return trace.Wrap(err)
			}
			fmt.Printf("%v\n", string(data))
			return nil
		default:
			if len(transactions.Items) == 0 {
				fmt.Printf("No transactions found\n")
				return nil
			}
			w := new(tabwriter.Writer)
			w.Init(os.Stdout, 0, 8, 1, '\t', 0)
			defer w.Flush()
			fmt.Fprintf(w, "Name\tStatus\tOperations\n")
			for _, tr := range transactions.Items {
				fmt.Fprintf(w, "%v\t%v\t%v\n", tr.Name, tr.Spec.Status, len(tr.Spec.Items))
			}
			return nil
		}
	}
	tr, err := tx.Get(ctx, namespace, ref.Name)
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
		fmt.Printf("Transaction %v in namespace %v\n\n", tr.Name, tr.Namespace)
		w := new(tabwriter.Writer)
		w.Init(os.Stdout, 0, 8, 1, '\t', 0)
		defer w.Flush()
		fmt.Fprintf(w, "Operation\tStatus\tDescription\n")
		for i, op := range tr.Spec.Items {
			var info string
			opInfo, err := rigging.GetOperationInfo(op)
			if err != nil {
				info = err.Error()
			} else {
				info = opInfo.String()
			}
			fmt.Fprintf(w, "%v\t%v\t%v\n", i, op.Status, info)
		}
		return nil
	}
}

func txDelete(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, namespace string, tr rigging.Ref) error {
	tx, err := rigging.NewTransaction(rigging.TransactionConfig{
		Client: client,
		Config: config,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	err = tx.Delete(ctx, namespace, tr.Name)
	if err != nil {
		return trace.Wrap(err)
	}
	fmt.Printf("%v has been removed\n", tr.Name)
	return nil
}

func printHeader(val string) {
	fmt.Printf("\n[%v]\n%v\n", val, strings.Repeat("-", len(val)+2))
}

// InitLoggerCLI tools by default log into syslog, not stderr
func InitLoggerCLI() {
	log.SetLevel(log.WarnLevel)
	// clear existing hooks:
	log.StandardLogger().Hooks = make(log.LevelHooks)
	log.SetFormatter(&trace.TextFormatter{})

	hook, err := logrusSyslog.NewSyslogHook("", "", syslog.LOG_WARNING, "")
	if err != nil {
		// syslog not available
		log.Warn("syslog not available. reverting to stderr")
	} else {
		// ... and disable stderr:
		log.AddHook(hook)
		log.SetOutput(ioutil.Discard)
	}
}

// InitLoggerDebug configures the logger to dump everything to stderr
func InitLoggerDebug() {
	// clear existing hooks:
	log.StandardLogger().Hooks = make(log.LevelHooks)
	log.SetFormatter(&trace.TextFormatter{})
	log.SetOutput(os.Stderr)
	log.SetLevel(log.DebugLevel)
}

const (
	inCluster  = "in"
	outCluster = "out"
)

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
