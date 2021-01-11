*Okta Scale Harness*

To start a new project:

create a new directory in /projects with the name of your project

in the new project dir, create a `secrets.json` file

In the database:

add a row to the projects table

copy one of the objects_* tables as:

objects_{{project_name}}

***make sure to set the primary index and to auto-increment***

To launch a new batch:

`python3 app.py -p cisco_300m_users`
