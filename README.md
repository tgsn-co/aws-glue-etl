# aws-glue-etl
In this repository you will find a folder per AWS Glue job.

### Important notes:

Notebook jobs do not support versioning controll directly from the AWS console. As such, the code written in a Notebook job, needs to be manually commited to this repo. To do so, follow the steps mentioned on the next sub-section.
Any other job types, as script and visual jobs, can be committed using the AWS console. The details of the repo need to be added to the versioning tab every time we log into AWS. The steps to commit the code in these cases is explained in the console.

### Commit Notebook jobs:

1 - If you are committing a job for the first time, start by creating a folder in the repo with the same name as the job. 

2 - After saving the notebook job, click 'Download Notebook', the third button to the left of the 'Run' button.

3 - Save the downloaded file in the corresponding repo folder, overwriting it if a file already exists.

4 - Commit the new code to github with an informative message, to the develop branch. 

All jobs should be committed to the develop branch by default. Once the job is ready to be used in a production setting a pull request (PR) can be made to the main branch.
