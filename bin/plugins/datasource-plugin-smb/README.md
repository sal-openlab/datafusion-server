# SMB/CIFS Data Source Connector Plugin

## Usage

## Development

### Pre-requirement

#### Python runtime

Required Python interpreter v3.7+

##### Linux (Ubuntu, Debian)

```sh
$ sudo apt install python3-dev
```

Additional requirements for SMB3(Kerberos)

```sh
$ sudo apt install libkrb5-dev
```

##### macOS

Additional requirements for SMB3(Kerberos)

```sh
$ brew install krb5
```

### Debugging

#### Prepare Virtual Environment

##### Create venv

```sh
$ python3 -m venv .venv
```

##### Activate venv

```sh
$ source venv/bin/activate
```

##### Install required packages for venv (if required)

```sh
$ pip install -r requirements.txt
```

##### Deactivate venv

```sh
$ deactivate
```
