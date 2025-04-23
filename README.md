# Data Extractors

## Description

Provides the extraction layer between the providers and a destination db (here Postgres)

## Getting Started

### Dependencies

* N/A

### Installing
* Note: Currently the version dlt==1.5.0 works with Python 3.11 (not Python 3.13)

* Git clone the project
    * git clone https://github.com/FredGH/extrator.git
* Ensure python3 is pointing to the 3.11
    * Go to https://www.python.org/downloads/ and download Python 3.11.x
    * Go to Visual Studio Code, select the Python Interpreter (Shift+Cmd+P) as Python 3.11.x
* Create the Python Env
    * rm -rf venv/
    * In Visual Studio Vode -> Ctrl+Shift+P -> Select Interpreter ->  Python 3.11.x
    * python3.11 -m venv venv
    * source ./venv/bin/activate
    * In Visual Studio Vode -> Ctrl+Shift+P -> Select Interpreter ->  (venv) Python 3.11.x
    * [optional] check Python3 version -> python3 -> should show Python 3.11.x
* Install Requirements:
    * python3.11 -m pip install --upgrade pip
* Build Package:
    * pip3.11 install -U pip setuptools
    * python3.11 setup.py sdist bdist_wheel
    * pip3.11 install -e .
    * [Optional]:
        * In case you get this error, then follows the resolution steps:
            * Error: "[...]Cargo, the Rust package manager, is not installed or is not on PATH. This package requires Rust and Cargo to compile extensions. [...]"
            * Resolution Steps: 
                * In the venv terminal:
                    * run curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
                    * . "$HOME/.cargo/env" 
                    *  pip3.11 install -e .
* Tag New Release & Push:
    * git tag 0.0.x -m "Release details"
    * git push origin 0.0.x
    * git push
* Install Package:
    *  Go to Settings > Developer Settings > Personal access tokens (classic) > Generate new token with note e.g. "Upload package"
    *  Ensure you check the write:packages scope to grant the necessary permissions.
    * Get the generated token, e.g. "12345"
    * Get your Github user name, e.g. "my_user_name"
        * Install the private package using the following:
        * Template:
            * pip3.11 install git+https://github.com/{{ username }}/{{ package name }}.git@{{ tag/version }}
            * [Optional] pip3.11 install git+https://{{ your access token }}@github.com/{{ username }}/{{ repository name}}.git@{{ tag/version }}#egg={{ package name }}
        * Example:
            * pip3.11 install git+https://github.com/{{ username }}/extractor.git@0.0.x
            * [Optional] pip3.11 install git+https://12345@github.com/{{ username }}/extrator.git@0.0.x#egg=extractor
    * [Optional] Deleting tags:
        * List tags: git tag -l
        * Delete origin: git push origin :refs/tags/{{tagname}} (e.g. 0.01)
        * Delete locally: git tag --delete {{tagname}} (e.g. 0.01)

* Code cleanup & Standardisation
    * ruff check . &isort . &black .

* Run unit tests
    * run in /extractor 
        * coverage run -m unittest discover
        * coverage report -m (or coverage hml)

#
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
