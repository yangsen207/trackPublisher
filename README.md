# Track Publisher
## Overview

A simple program to read EUROCONTROL CAT62 tracks information from a file, convert to json and publish to Solace broker

## Build the Project
This project requires maven to build
  1. clone this Github repository
  2. mvn clean install
  
## Running the Project
	java -jar trackPublisher-0.0.1-SNAPSHOT-jar-with-dependencies.jar <file-path> <host:port> <message-vpn> <client-username> <password> [msg-rate]

[msg-rate] is the number of tracks data to publish per second. Default value is 5.	

## License

This project is licensed under the Apache License, Version 2.0. - See the [LICENSE](LICENSE) file for details.


## Resources

If you want to learn more about Solace Technology try these resources:

- The Solace Developer Portal website at: http://dev.solace.com
- Get a better understanding of [Solace technology](http://dev.solace.com/tech/).
- Check out the [Solace blog](http://dev.solace.com/blog/) for other interesting discussions around Solace technology
- Ask the [Solace community.](http://dev.solace.com/community/)
