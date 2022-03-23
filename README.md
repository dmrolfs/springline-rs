# springline #

----
### Autoscale Flink ###

## Motivation ##  
Springline provides highly tailorable and robust autoscaling to Flink clusters. Flink does not 
autoscale its cluster for workload, leaving users to choose between over-provisioning the cluster or
living with a possibly unhealthy pipeline. Data processing can be unrelenting for pipelines. Flink 
effectively handles heavy data processing loads. Workloads vary over time; however, and without an 
autoscaling solution Flink is unable to adjust accordingly. Without considering load, the user may 
have an under-provisioned cluster at times, which results in an unhealthy lag seen in the input; 
e.g., Kafka or Kinesis. Alternatively, the user may scale the cluster for the anticipated  maximum 
load, but this can be very expensive and wasteful since many of the workers are left underutilized 
as the workload dips.
  
It is possible to rescale the Flink cluster with workload, but since Flink jobs can tackle 
different types of problems, a single metric or approach may not be best for every situation. 
Several articles (1, 2, 3, 4) are available that describe what to consider when rescaling a Flink 
system and some offer proprietary or future solutions.  However, each focus on a limited area of 
concern or require you to do the difficult part to make the scaling decision and to set the target 
level.

Traditionally, scaling Flink involves three steps: 
1) Take a Savepoint and stop the Flink job, 
2) rescale the cluster, and 
3) resume Flink job with the Savepoint and adjusted parallelism. 

This process is time consuming and can take many minutes — all the while records keep buffering at 
the input. Released with version 1.13, Reactive Flink dynamically adjusts parallelism to match the 
available TaskManagers. This helps a lot by substantially reducing by avoiding the need to take a 
savepoint and resume the pipeline, but Reactive Flink does not help you further. You are left to 
determine when to scale; you need to calculate how much to scale; and, you must execute the 
rescaling action. Furthermore, Reactive Flink is limited to standalone mode has not advanced in 
maturity over several minor releases since its MVP release with Flink 1.13. There has been some talk
about adding Flink autoscaling onto Reactive Flink, but that feature has not made it to Flink’s 
roadmap. 
  
Springline is a small, yet highly tailorable process that deploys and integrates with the Flink 
system. Springline actively works to keep Flink jobs healthy by sensing multiple conditions of the 
Flink pipeline and its (Kubernetes) environment, and employing highly tailorable, business-oriented 
policies to make decisions.

----

## Getting Started ##
### Project Structure ###
The project is organized with the following folders. Refer to corresponding code modules for more 
information about the implementation:

#### springline ####
Main source codebase and integration tests for the springline process. Springline is built in terms 
of proctor, a library of asynchronous stage processing. Additionally, springline planning phases, 
in particular Decision, center around policies written in the Polar logic language. Originally 
intended for authorization policies, Polar can be used independently. (Thanks go out to the very 
responsive and helpful team at Oso!)

#### springline-explorer ####
This is an experimental, very much work-in-progress CLI application that aspires to help springline 
policy development.

#### springline-transcribe ####
A simple CLI to convert a springline configuration file from one format into another. 

#### resources ####
Initial set of configuration files and policy files for planning phases. 
* __Settings__: [todo - springline settings hierarchy] and options
* __Policies__ : [todo - springline planning phase policies and template structure]

#### scripts ####
Contains shell scripts that can be used to build and run the springline docker container.

### Building Locally ###
From either the base or `./springline` directories you can use [cargo](https://doc.rust-lang.org/cargo/)
to compile, build and run locally. (Running requires corresponding configuration.)

* To run all of the unit, integration and doc tests using: `cargo test`
* To compile the release binary: `cargo build --release`

### Running Locally ###
Springline expects that configuration is mainly done via configuration and policy files, however it
does accept a few command line options, which mainly drive what configuration to load. You may see 
see the CLI usage page via the usual means:

```sh
❯ ./target/debug/springline --help
springline 0.10.1
Damon Rolfs <drolfs@gmail.com>
Autoscaling driver for Flink servers.

USAGE:
springline [OPTIONS]

OPTIONS:
-c, --config <CONFIG>              override environment-based configuration file to load.
Default behavior is to load configuration based on
`APP_ENVIRONMENT` envvar
-e, --environment <ENVIRONMENT>
-h, --help                         Print help information
-m, --machine-id <MACHINE_ID>      Specify the machine id [0, 31) used in correlation id
generation, overriding what may be set in an environment
variable. This id should be unique for the entity type within
a cluster environment. Different entity types can use the
same machine id
-n, --node-id <NODE_ID>            Specify the node id [0, 31) used in correlation id
generation, overriding what may be set in an environment
variable. This id should be unique for the entity type within
a cluster environment. Different entity types can use the
same machine id
-r, --resources <RESOURCES>        Override default location from which to load configuration
files. Default directory is ./resources
-s, --secrets <SECRETS>            specify path to secrets configuration file
-V, --version                      Print version information

```

### Docker ###
#### Building ####
Springline’s Dockerfile can be found at ./springline/Dockerfile. You can build the container using 
the script:
```sh
> . ./scripts/build_springline_docker.sh
```

The dockerfile follows a staged structure, which builds the binary in a container stage and then 
creates a minimal container for production. Because the build process is self-contained, the rust 
toolchain is not required in the immediate build environment.

#### Running ####
The springline dockerfile has a couple expectations that must be satisfied in the container 
environment.

## Autoscaling Big Picture ##

## Senses Phase ##

## Planning Phase ##

### Policies ###

### Eligibility ###

### Decision ###

### Forecast ###

### Governance ###

## Act Phase ##