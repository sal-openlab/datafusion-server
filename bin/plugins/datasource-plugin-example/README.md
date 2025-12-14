# Example for datafusion-server data source connector plugin

## Usage

## Development

### Pre-requirement

#### Python runtime

Required Python interpreter v3.7+

* Linux (Ubuntu, Debian)

```sh
$ sudo apt install python3-dev
```

* Windows
    - https://www.python.org/downloads/windows/

### Debugging

#### Prepare Virtual Environment

##### Create venv

```sh
$ python3 -m venv .venv
```

##### Activate venv

* Linux, macOS, Unix

```sh
$ source venv/bin/activate
```

* Windows

```
> .\venv\Scripts\activate
```

##### Install required packages for venv (if required)

```sh
$ pip install -r requirements.txt
```

##### Deactivate venv

```sh
$ deactivate
```

#### Execute debugging module in venv

* Standalone

```sh
$ python plugin_main.py
```

* Unit Test

```sh
$ python -m unittest test_main.py
```
