module github.com/gravitational/rigging

go 1.12

require (
	github.com/coreos/prometheus-operator v0.0.0-00010101000000-000000000000
	github.com/davecgh/go-spew v1.1.1
	github.com/ghodss/yaml v1.0.0
	github.com/googleapis/gnostic v0.1.0 // indirect
	github.com/gravitational/trace v1.1.6-0.20180717152918-4a5e142f3251
	github.com/imdario/mergo v0.3.6 // indirect
	github.com/kylelemons/godebug v0.0.0-20170820004349-d65d576e9348
	github.com/sirupsen/logrus v1.4.2
	golang.org/x/crypto v0.0.0-20200220183623-bac4c82f6975 // indirect
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4 // indirect
	gopkg.in/airbrake/gobrake.v2 v2.0.9 // indirect
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15
	gopkg.in/gemnasium/logrus-airbrake-hook.v2 v2.1.2 // indirect
	k8s.io/api v0.16.15
	k8s.io/apiextensions-apiserver v0.0.0-20191016113550-5357c4baaf65
	k8s.io/apimachinery v0.16.15
	k8s.io/client-go v12.0.0+incompatible
	k8s.io/kube-aggregator v0.0.0-20191016112429-9587704a8ad4
)

replace k8s.io/client-go => k8s.io/client-go v0.0.0-20191016111102-bec269661e48

replace github.com/sirupsen/logrus => github.com/gravitational/logrus v0.10.1-0.20180402202453-dcdb95d728db

replace github.com/coreos/prometheus-operator => github.com/gravitational/prometheus-operator v0.35.2
