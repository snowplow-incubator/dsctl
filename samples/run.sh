# Sample script to take a data structure from validation to production
# For the following to work, a .env file needs to be present on the same location,
# setting all the environment variables that the script expects to resolve

check_status() {
  status=$?
  action=$1
  if [ $status -ne 0 ]; then
      echo $action
      exit $status
  fi
}

# Asssuming a virtual env named `dsctl`
source $WORKON_HOME/dsctl/bin/activate

# Grab a token at the beginning and reuse it throughout
token=$(python ../dsctl.py --token-only)

# Default action is to validate. This will reuse the token, but if you omit the `token`
# parameter a new one will be acquired automatically.
python ../dsctl.py --file sample.json --token $token --type entity
check_status "Could not validate data structure"

# Promote to dev. Schema is required as input only to extract self-description
python ../dsctl.py --file sample.json --token $token --promote-to-dev --message "Promoting 1-0-0 to dev"
check_status "Could not promote data structure to development environment"

# Promote to prod.
python ../dsctl.py --file sample.json --token $token --promote-to-prod --message "Promoting 1-0-0 to prod"
check_status "Could not promote data structure to production environment"

# We will now update schema to version 1-0-1. In this case we'll use a file
# that already contains metadata (otherwise added automatically with sensible
# defaults by the script).
# There is no requirement to use input with or without meta; you only need to
# remember to pass `--includes-meta` if the input does contain that section.
python ../dsctl.py --file sample-with-meta.json --token $token --includes-meta
check_status "Could not validate data structure"

# Promote to dev. Schema is required as input only to extract self-description
python ../dsctl.py --file sample-with-meta.json --token $token --includes-meta --promote-to-dev --message "Promoting 1-0-1 to dev"
check_status "Could not promote data structure to development environment"

# Promote to prod.
python ../dsctl.py --file sample-with-meta.json --token $token --includes-meta --promote-to-prod --message "Promoting 1-0-1 to prod"
check_status "Could not promote data structure to production environment"
