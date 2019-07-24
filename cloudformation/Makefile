TEMPLATES=main collector-pipes-apig collector-ga collector-ga-monitoring 

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
		aws cloudformation validate-template --template-body file://$$i.yaml; \
		done
	@echo All cloudfromation files are valid

ga_deploy: validate
	@if [ ! -d 'temp/ga' ]; then \
		 mkdir -p temp/ga; \
	fi
	@rm -rf temp/ga && mkdir -p temp/ga
	@aws cloudformation package --template-file $(GA_STACK_FILE) --output-template-file temp/ga/output.yaml --s3-bucket $(BUCKET) --region eu-central-1
	@aws cloudformation deploy --template-file temp/ga/output.yaml --stack-name $(GA_STACKNAME) --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --region eu-central-1

mon_deploy: validate
	@if [ ! -d 'temp/monitoring' ]; then \
		 mkdir -p temp/monitoring; \
	fi
	@rm -rf temp/monitoring && mkdir -p temp/monitoring
	@aws cloudformation package --template-file $(MONITORING_TEMPLATE) --output-template-file temp/monitoring/output.yaml --s3-bucket $(BUCKET) --region eu-central-1
	@aws cloudformation deploy --template-file temp/monitoring/output.yaml --stack-name $(MONITORING_STACKNAME) --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --region eu-central-1

create_bucket:
	aws s3api create-bucket --bucket $(BUCKET) --region eu-central-1 --create-bucket-configuration LocationConstraint=eu-central-1

main_deploy: validate
	@if [ ! -d 'temp/main' ]; then \
		 mkdir -p temp/main; \
	fi
	@rm -rf temp/main && mkdir -p temp/main
	@aws cloudformation package --template-file $(MAIN_TEMPLATE) --output-template-file temp/main/output.yaml --s3-bucket $(BUCKET) --region eu-central-1
	@aws cloudformation deploy --template-file temp/main/output.yaml --stack-name $(MAIN_STACKNAME) --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --region eu-central-1

test_deploy: 
	@if [ ! -d 'temp/test' ]; then \
		 mkdir -p temp/test; \
	fi
	@rm -rf temp/test && mkdir -p temp/test
	@aws cloudformation package --template-file test-main.yaml --output-template-file temp/test/output.yaml --s3-bucket $(BUCKET) --region eu-central-1
	@aws cloudformation deploy --template-file temp/test/output.yaml --stack-name tarasowski-test --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --region eu-central-1
