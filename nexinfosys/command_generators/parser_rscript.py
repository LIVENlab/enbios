"""
Functionality to process an R script

* Prepare context for the execution:
  - special variables
  - state
* Execution:
  - the script, which has been designed to work accessing the RESTful API, will be executed locally
  - Instead of being an agent generating commands, it is itself a commands generator
  - Sandbox: no graphical functions, no filesystem operations, no output, only command execution API calls allowed (no NEW case study or similar, because the script will be in a case study), ...
  - no need to REGISTER, just MODIFY STATE. There must be a way for the endpoint to support this two behaviors

* After execution


"""