# pre install dependency
    pyenv
    pipenv
# create virtual env
    $pipenv --python=3.9

# activate virtual env
    $pipenv shell

# install dependency
    $pipenv install

# run test
    $python3 -m unittest test_assignment.py

# run script
    $python3 assignment.py

# if want to upload extracted csv to aws S3 then run script as 
    $python3 assignment.py --aws_access_key_id XXXXX --aws_secret_access_key XXXX --region_name XXXXX --bucket XXXXX

# generate pydoc file
    $pydoc -w assignment

