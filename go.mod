module github.com/gravitational/rigging

go 1.12

require (
	github.com/davecgh/go-spew v1.1.1
	github.com/ghodss/yaml v1.0.0
	github.com/gravitational/trace v1.1.6-0.20180717152918-4a5e142f3251
	github.com/imdario/mergo v0.3.6 // indirect
	github.com/kylelemons/godebug v0.0.0-20170820004349-d65d576e9348
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring v0.50.0
	github.com/prometheus-operator/prometheus-operator/pkg/client v0.50.0
	github.com/sirupsen/logrus v1.6.0
	gopkg.in/airbrake/gobrake.v2 v2.0.9 // indirect
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f
	gopkg.in/gemnasium/logrus-airbrake-hook.v2 v2.1.2 // indirect
	k8s.io/api v0.19.8
	k8s.io/apiextensions-apiserver v0.19.8
	k8s.io/apimachinery v0.22.0
	k8s.io/client-go v12.0.0+incompatible
	k8s.io/kube-aggregator v0.19.8
	k8s.io/utils v0.0.0-20210820185131-d34e5cb4466e
)

replace (
	github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring => github.com/gravitational/prometheus-operator/pkg/apis/monitoring v0.43.1-0.20210818162409-2906a7bf1935
	github.com/prometheus-operator/prometheus-operator/pkg/client => github.com/gravitational/prometheus-operator/pkg/client v0.0.0-20210818162409-2906a7bf1935
	github.com/sirupsen/logrus => github.com/gravitational/logrus v0.10.1-0.20180402202453-dcdb95d728db
	k8s.io/apimachinery => k8s.io/apimachinery v0.19.8
	k8s.io/client-go => k8s.io/client-go v0.19.8
)
