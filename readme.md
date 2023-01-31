# Artifact Emulator Application for eGSM Monitoring Platform
This application is capable to emulate an arbitrarily number of smart objects, embodying business artifacts. In addition, the application emulates the Organizational Information System as well, therefore it sends out messages to attach and detach artifacts from different process executions.

## Requirements
The Artifact Emulator requires:
1. MQTT broker using at least MQTT v5.0

## Configuration
The configuration should be provided as an .xml file, referenced below as `config.xml`. (example: `shipment-1-data/config.xml`)
The configuration file defines:
1. The details of the MQTT broker,
2. Lists all artifacts and their associated event stream files, containing all events with timestamps coming from the particular artifact
3. Defines all process instances and their associated event stream file, which contains all events with timestamps related to the process instance coming from the Organizational Information Network


## Usage
1. Clone repository
2. Run `git submodule update --init`
3. Run `npm install package.json` and make sure all libraries have been installed successfully
4. Start the application by `node main.js config.xml`