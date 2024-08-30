default: help

##@ Utility
help:  ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

btcreate: ## Spin up a single node BigTable 
	@gcloud bigtable instances create timeseries --display-name="Timeseries BigTable" --cluster-config=zone=us-west1-a,nodes=1,id=timeseries-cluster

btdelete: ## Shutdown the BigTable 
	@gcloud bigtable instances delete timeseries

