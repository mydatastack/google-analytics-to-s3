TEMPLATES=main collector-ga collector-ga-monitoring 

# Google Duplicator Deplyoment Variables
GA_STACK_FILE=collector-ga.yaml
MONITORING_TEMPLATE=collector-ga-monitoring.yaml
BUCKET=pipes-cf-artifacts
GA_STACKNAME=tarasowski-ga-dev-machine
MONITORING_STACKNAME=tarasowski-ga-monitoring-dev-machine
MAIN_TEMPLATE=main.yaml
MAIN_STACKNAME=tarasowski-main-dev-machine
REGION=eu-central-1
#-----

validate:
	@for i in $(TEMPLATES); do \
		aws cloudformation validate-template --template-body file://cloudformation/$$i.yaml; \
		done
	@echo All cloudfromation files are valid

ga_deploy: validate
	@if [ ! -d './cloudformation/temp/ga' ]; then \
		 mkdir -p ./cloudformation/temp/ga; \
	fi
	@rm -rf ./cloudformation/temp/ga && mkdir -p ./cloudformation/temp/ga
	@aws cloudformation package --template-file ./cloudformation/$(GA_STACK_FILE) --output-template-file ./cloudformation/temp/ga/output.yaml --s3-bucket $(BUCKET) --region eu-central-1
	@aws cloudformation deploy --template-file ./cloudformation/temp/ga/output.yaml --stack-name $(GA_STACKNAME) --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --region eu-central-1

monitoring_deploy: validate
	@if [ ! -d './cloudformation/temp/monitoring' ]; then \
		 mkdir -p ./cloudformation/temp/monitoring; \
	fi
	@rm -rf ./cloudformation/temp/monitoring && mkdir -p ./cloudformation/temp/monitoring
	@aws cloudformation package --template-file ./cloudformation/$(MONITORING_TEMPLATE) --output-template-file ./cloudformation/temp/monitoring/output.yaml --s3-bucket $(BUCKET) --region eu-central-1
	@aws cloudformation deploy --template-file ./cloudformation/temp/monitoring/output.yaml --stack-name $(MONITORING_STACKNAME) --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --region eu-central-1

create_bucket:
	aws s3api create-bucket --bucket $(BUCKET) --region eu-central-1 --create-bucket-configuration LocationConstraint=eu-central-1

deploy: validate
	@if [ ! -d './cloudformation/temp/main' ]; then \
		 mkdir -p ./cloudformation/temp/main; \
	fi
	@rm -rf ./cloudformation/temp/main && mkdir -p ./cloudformation/temp/main
	@aws cloudformation package --template-file ./cloudformation/$(MAIN_TEMPLATE) --output-template-file ./cloudformation/temp/main/output.yaml --s3-bucket $(BUCKET) --region eu-central-1
	@aws cloudformation deploy --template-file ./cloudformation/temp/main/output.yaml --stack-name $(MAIN_STACKNAME) --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --region eu-central-1

