# dataflow-template

### A templatable data cleaning pipeline, extracting the path to a csv file then cleaning the given information

### To Run:

- Fork and download this repo

- Use the Google SDK prompt ([download if needed](https://cloud.google.com/sdk/)), move to folder of repo and to create a template, input:

`python run_pipeline.py --requirements_file requirements.txt --runner DataflowRunner --staging_location gs://bucket/staging/location --temp_location gs://bucket/temp/location --template_location gs://bucket/template/location/name --project project-name --extra_package ./beam_utils/dist/beam_utils-0.0.4.zip`


### Runtime Parameters

-Upon launching the template within the dataflow console, first it will be necessary to input the template in the custom template gcs path:

    -gs://path/to/template
    
within the Additional parameters it will be possible to input:

    - path : gs://path/to/file
    - source : source-name

    
##### Execute with HTTP Request

Requires Authentication

An Example can be seen [here](https://github.com/jamesmoore255/dataflow_template/blob/master/post_request.py)
