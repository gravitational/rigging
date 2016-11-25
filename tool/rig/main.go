package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log/syslog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/gravitational/rigging"

	log "github.com/Sirupsen/logrus"
	logrusSyslog "github.com/Sirupsen/logrus/hooks/syslog"
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

		debug      = app.Flag("debug", "turn on debug logging").Default("true").Bool()
		mode       = app.Flag("mode", "mode to run: 'in' - for running inside the k8s cluster, 'out' to use outside cluster").Default(outCluster).String()
		kubeConfig = app.Flag("kubeconfig", "path to kubeconfig").Default(filepath.Join(os.Getenv("HOME"), ".kube", "config")).String()
		namespace  = app.Flag("namespace", "Namespace of the transactions").Default("default").String()

		cupsert               = app.Command("upsert", "Upsert resources in the context of a transaction")
		cpsertTransactionName = cupsert.Arg("transaction", "name of the transaction").Required().String()
		cupsertFile           = cupsert.Flag("file", "file with new spec").Short('f').Required().String()

		ctsatus     = app.Command("status", "Check status of all operations in a transaction")
		cstatusName = cstatus.Arg("resource", "resource to check, e.g. tx/tx1").Required().String()

		crollback     = app.Command("rollback", "Rollback the transaction")
		crollbackName = crollback.Arg("transaction", "name of the transaction").Required().String()

		cds = app.Command("ds", "operations on daemon sets")

		cdsUpdate     = cds.Command("update", "Rolling update daemon set")
		cdsUpdateFile = cdsUpdate.Flag("file", "file with new daemon set spec").Short('f').Required().String()

		cdsStatus         = cds.Command("status", "Check status of a daemon set")
		cdsStatusFile     = cdsStatus.Flag("file", "file with new daemon set spec").Short('f').Required().String()
		cdsStatusAttempts = cdsStatus.Flag("retry-atempts", "file with new daemon set spec").Default(fmt.Sprintf("%v", rigging.DefaultRetryAttempts)).Int()
		cdsStatusPeriod   = cdsStatus.Flag("retry-period", "file with new daemon set spec").Default(fmt.Sprintf("%v", rigging.DefaultRetryPeriod)).Duration()

		ctr = app.Command("tx", "Operations on transactions")

		ctrRemove     = ctr.Command("rm", "Remove transaction")
		ctrRemoveName = ctrRemove.Arg("name", "Transaction name").Required().String()

		ctrList = app.Command("ls", "List transactions")
	)

	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		return trace.Wrap(err)
	}

	if *debug {
		InitLoggerDebug()
	}

	var client *kubernetes.Clientset
	var config *rest.Config
	switch *mode {
	case inCluster:
		fmt.Printf("using in cluster config\n")
		// creates the in-cluster config
		config, err = rest.InClusterConfig()
		if err != nil {
			return trace.Wrap(err)
		}
		client, err = kubernetes.NewForConfig(config)
		if err != nil {
			return trace.Wrap(err)
		}
	case outCluster:
		config, err = clientcmd.BuildConfigFromFlags("", *kubeConfig)
		if err != nil {
			return trace.Wrap(err)
		}
		client, err = kubernetes.NewForConfig(config)
		if err != nil {
			return trace.Wrap(err)
		}
	default:
		return trace.BadParameter("unsupported mode: %v", *mode)
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
	case cdsUpdate.FullCommand():
		return updateDaemonSet(ctx, client, *cdsUpdateFile)
	case cdsStatus.FullCommand():
		return statusDaemonSet(ctx, client, *cdsStatusFile, *cdsStatusAttempts, *cdsStatusPeriod)
	case ctrList.FullCommand():
		return txList(ctx, client, config, *namespace)
	case ctrRemove.FullCommand():
		return txRemove(ctx, client, config, *namespace, *ctrRemoveName)
	case cstatus.FullCommand():
		return txStatus(ctx, client, config, *namespace, *ctrStatusName)
	case ctrRollback.FullCommand():
		return txRollback(ctx, client, config, *namespace, *ctrRollbackName)
	case cdsUpsert.FullCommand():
		return upsert(ctx, client, config, *namespace, *cdsUpsertTransactionName, *cdsUpsertFile)
	}

	return trace.BadParameter("unsupported command: %v", cmd)
}

func upsert(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, namespace, transactionName, filePath string) error {
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
	err = tx.Upsert(ctx, namespace, transactionName, data)
	if err != nil {
		return trace.Wrap(err)
	}
	fmt.Printf("transaction %v applied \n", transactionName)
	return nil
}

func txList(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, namespace string) error {
	tx, err := rigging.NewTransaction(rigging.TransactionConfig{
		Client: client,
		Config: config,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	transactions, err := tx.List(ctx, namespace)
	if err != nil {
		return trace.Wrap(err)
	}
	for _, tr := range transactions.Items {
		fmt.Printf("* %v %v\n", tr.Name, tr.Spec.Items)
	}
	return nil
}

func txRemove(ctx context.Context, client *kubernetes.Clientset, config *rest.Config, namespace string, name string) error {
	tx, err := rigging.NewTransaction(rigging.TransactionConfig{
		Client: client,
		Config: config,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	err = tx.Delete(ctx, namespace, name)
	if err != nil {
		return trace.Wrap(err)
	}
	fmt.Printf("%v has been removed\n", name)
	return nil
}

func updateDaemonSet(ctx context.Context, client *kubernetes.Clientset, filePath string) error {
	data, err := ReadPath(filePath)
	if err != nil {
		return trace.Wrap(err)
	}
	updater, err := rigging.NewDSUpdater(rigging.DSConfig{
		Reader: bytes.NewBuffer(data),
		Client: client,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	return updater.Update(ctx)
}

func statusDaemonSet(ctx context.Context, client *kubernetes.Clientset, filePath string, retryAttempts int, retryPeriod time.Duration) error {
	data, err := ReadPath(filePath)
	if err != nil {
		return trace.Wrap(err)
	}
	updater, err := rigging.NewDSUpdater(rigging.DSConfig{
		Reader: bytes.NewBuffer(data),
		Client: client,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	return updater.Status(ctx, retryAttempts, retryPeriod)
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
