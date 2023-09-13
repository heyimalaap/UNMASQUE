
# UN1
This code corresponds to UNMASQUE Project: https://dsl.cds.iisc.ac.in/projects/HIDDEN/index.html
UNMASQUE code for https://dsl.cds.iisc.ac.in/publications/report/TR/TR-2021-02_updated.pdf  
This repository contains code developed as part of various theses in UN1. 
The code is organized into the following directories:

## mysite

The `mysite` directory contains the main project code.

### unmasque

Inside `unmasque`, you'll find the following subdirectories:

#### refactored

The `refactored` directory contains code that has been refactored from the original codebase developed in various theses. This code has likely undergone improvements and optimizations.

#### src

The `src` directory contains newly written logic, often designed to simplify existing code. This may include enhancements or entirely new functionality.

#### test

The `test` directory houses unit test cases for each extractor module. These tests are crucial for ensuring the reliability and correctness of the code.

Please explore the individual directories for more details on the code and its purpose.

## Usage

### Configuration
inside `mysite` directory, there are three files as follows:  
config.ini --> This contains database login credentials. Change the fields accordingly.  
create_indexes.sql --> currently empty.  (do not delete this file.)
pkfkrelations.csv --> contains key details for the TPCH schema. If any other schema is to be used, change this file accordingly.

### Running Unmasque
Open `mysite/unmasque/src/my_cmd.py` file. 
This script has one default input specified. 
Change this query to try Unmasque for various inputs. 
Use the following command:
`python my_cmd.py`


