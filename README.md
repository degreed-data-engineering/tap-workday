# `tap-workday`
This tap workday was created by Degreed as a workday to be used for extracting data via Meltano into defined targets

# Configuration required:

```python
    config_jsonschema = th.PropertiesList(
        th.Property("username", th.StringType, required=False, description="usernameh"),
        th.Property("password", th.StringType, required=False, description="password"),
    ).to_dict()
```
## Testing locally

To test locally, pipx poetry
```bash
pipx install poetry
```

Install poetry for the package
```bash
poetry install
```

To confirm everything is setup properly, run the following: 
```bash
poetry run tap-workday --help
```

To run the tap locally outside of Meltano and view the response in a text file, run the following: 
```bash
poetry run tap-workday > output.txt 
```

A full list of supported settings and capabilities is available by running: `tap-workday --about`
