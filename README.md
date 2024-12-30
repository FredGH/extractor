# Project Title

Data Providers

## Description

Provides access to external data providers data, such as yahoo financial data.

## Getting Started

### Dependencies

* N/A

### Installing

* Git clone the project
    * git clone https://github.com/FredGH/data_providers.git
* Create the Python Env
    * python3 -m venv venv
    * source ./venv/bin/activate
* Install Requirements:
    * python3 -m pip install --upgrade pip
    * pip3 install -r requirements.txt
* Build Package:
    * python3 setup.py sdist bdist_wheel
    * pip3 install -e .
* Tag New Release & Push:
    * git tag 0.0.x -m "Release details"
    * git push origin 0.0.x
* Install Package:
    *  Go to Settings > Developer Settings > Personal access tokens (classic) > Generate new token with note e.g. "Upload package"
    *  Ensure you check the write:packages scope to grant the necessary permissions.
    * Get the generated token, e.g. "12345"
    * Get your Github user name, e.g. "my_user_name"
    * Install the private package using the following:
        * Template:
            * pip install git+https://{{ your access token }}@github.com/{{ username }}/{{ repository name}}.git@{{ tag/version }}#egg={{ package name }}
            * pip install git+https://github.com/{{ username }}/{{ package name }}.git@{{ tag/version }}
        * Example:
            * pip install git+https://12345@github.com/my_user_name/data_providers.git@0.0.1#egg=data_providers
            * pip install git+https://github.com/my_user_name/data_providers.git@0.0.1
    
### Executing program

* N/A

## Help

* N/A

## Authors

Contributors names and contact info
freddy.marechal@gmail.com

## Version History

* 0.0.1
    * First version only containing stubs
* 0.0.2
    * Initial Release

## License

* There is no license.

## Acknowledgments

Inspiration, code snippets, etc.
* [Create and Release Private Python Package On Github](https://dev.to/abdellahhallou/create-and-release-a-private-python-package-on-github-2oae)
