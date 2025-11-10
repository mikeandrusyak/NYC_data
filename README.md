# Configuring the Project 
To configure the Project that all works out of the box there are some dependencies to install 
 
## Macos Path 

Install Homebrew, if not installed: https://brew.sh

Then install make:
<br>

`
brew install make
`
<br>
`
gmake --version
`

Make sure to install Python 3.12.2 
If pyenv is not installed then do it like that:

`
brew install python@3.12
`

### Pyenv 

If pyenv is installed use:

`
pyenv install 3.12.2
`

And then in the Repository Directory

`
pyenv local 3.12.2
`


## Windows Path

Install the Python Version here: https://www.python.org/downloads/release/python-3122/

If pyenv is installed use the same commands as in the Macos Path for the Pyenv section

To install make on windows use the following manual:

In Powershell use:

`iwr -useb get.scoop.sh | iex`

Then:

`
scoop install make
`


And then test with:

`make --version`