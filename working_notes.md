### Questions:

- do we actually see phone numbers from multiple country codes?
- import_history... remove it?
  - just handle import_new

## Plans:

- abstract db interface ... sqlalchemy or sqlmodel
- on init, populate locations
- base model define get by id, get all, update by id
- build out dataframes while running, keeping failed records, save to csv after with new column of "error type"
- folder for input, output, errors, segments, reports
  - why or why not a message was received
- better env handling (be able to define constants in it from header as well as s3 stuff) -- this can be an import config
- import all events to dataframe in lambda_handler, then run through all new, then all resend, then all historical

## Architecture:

- models -- patient, phone, email, visit, location, message
  - location: mostly static, but we don't know how frequently they may/not update and change
- utils:
  - utils -- phone number checking, etc, pinpoint job
  - logging -- add s3 logging
  - exceptions
    - big worry is run-out-of-time exception
  - enums - reason_codes, for start
- config -- for env vars, other stuff on old lambda_function header
- db -- handles db connection and session
- lambda_function -- rest of main function

### Libraries:

- pydantic
- pandas

# Questions for next PR:

- what is the purpose of convert_sql_datetime (line 383)
