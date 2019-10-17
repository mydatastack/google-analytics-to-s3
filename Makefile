TEMPLATES=main collector-ga collector-ga-monitoring 

STACK ?= pipes-ga-to-s3 
BUCKET ?= pipes-cf-artifacts

create_bucket:
	aws s3api create-bucket --bucket $(BUCKET) --region eu-central-1 --create-bucket-configuration LocationConstraint=eu-central-1

validate:
	@for i in $(TEMPLATES); do \
		aws cloudformation validate-template --template-body file://cloudformation/$$i.yaml; \
		done
	@echo All cloudfromation files are valid

deploy: validate
	@if [ ! -d './cloudformation/temp/main' ]; then \
		 mkdir -p ./cloudformation/temp/main; \
	fi
	@rm -rf ./cloudformation/temp/main && mkdir -p ./cloudformation/temp/main
	@aws cloudformation package --template-file ./cloudformation/main.yaml --output-template-file ./cloudformation/temp/main/output.yaml --s3-bucket $(BUCKET) --region eu-central-1
	@aws cloudformation deploy --template-file ./cloudformation/temp/main/output.yaml --stack-name $(STACK) --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --region eu-central-1
	@rm -rf ./cloudformation/temp
