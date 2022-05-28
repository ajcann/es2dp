# Elicit Semantic Search Data Pipeline Take Home


## Getting started

Python ^3.9 is recommended for best compatibility with Ray.

This project uses [Poetry](https://python-poetry.org/) for dependency management though I have provided a requirements file as well for convienence.

If using Poetry, run `poetry shell` to create and activate a virtual environment from the currently used version of Python

To install dependencies with Poetry
`poetry install` from the project top-level direction.

If using pip and requirements file then proceed as normally.

## Ray

If you'll be using an AWS-based Ray Cluster run (the configuration is very basic and not optimized for the task e.g. I have not created a custom Docker container with the required dependecies.)

`ray up ray-cluster-config.yaml`

This will take a few minutes. In a more realistic scenario, the Ray cluster would likely live within a pre-existing Kubernetes cluster. To shut down and clean up the cluster run:

`ray down ray-cluster-config.yaml`

Note for submitting job to Ray cluster, you'll need to pass in the S2AG API Key.
`ray submit ray-cluster-config.yaml ./es2dp/s2ag.py -- --key <S2AG API KEY>`


## Contents

*ray-cluster-config.yaml*
AWS-based Ray Cluster config - very basic configuration for demo purposes

*./data/openalex*
Sample data of `Works` entity

*./data/s2ag*
Sample data of Abstracts, Citations, Papers. Files prepended with `mod-` have an altered and fake corpusid to create sample data which can be joined

*./e2dp/fake_sample.py*
Generate above mod- files

*./e2dp/openalex.py*
Just exploring the OpenAlex Works entity and what types of parsing/processing is required to reshape

*./e2dp/s2ag.py*
Distributed collection and transformation, via Ray, of S2AG Abstracts, Citations, Papers chunks
Note: Either set the `S2AG_API_KEY` environment variable or run program with --key argument.
