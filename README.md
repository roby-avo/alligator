# Alligator: Entity Linking Tool over tabular data
A.L.L.I.G.A.T.O.R. - Automated Learning and Linking for Intelligent Graph-based Association of Tabular Objects and Relationships

![diagram](https://github.com/roby-avo/alligator/assets/64791054/826191d6-df5c-4c6f-9409-751f5a649532)
Annotating tables with high quality semantic labels and linking cells in these tables to entities in reference knowledge
graphs (KGs) serve several downstream applications such as knowledge graph construction, data enrichment, data analytics, and so on. There is an increasing interest in combining algorithms for the automatic annotation of tables with interactive
applications that let users control and improve the annotations and use them for data enrichment. In this paper we propose a supervised entity linking network that behaves as a re-ranker and score normalizer of for scores returned by off-the-shelves entity retrieval systems. The scores capture link uncertainty and
can therefore be used to let users validate more uncertain links first and increase the overall quality of the links faster. Experi-
ments suggest that the proposed approach provides an effective approach to identify most critical decisions and support link revision within a human-in-the-loop data enrichment paradigm.

![alligator_pipeline](https://github.com/roby-avo/alligator/assets/64791054/71a94186-d1ec-4c7e-b10f-7417d8171054)

Alligator is a versatile tool designed to perform entity linking on tabular data, leveraging machine learning techniques to optimize the process. This tool is intended to reconcile named entity columns (NE-columns) against a Knowledge Graph and also identify literal columns (LIT-columns) which may contain literal values such as numbers, dates, or generic text.

The Alligator pipeline consists of the following steps:

1. **Data Analysis & Pre-processing**: In this initial phase, the columns are categorized into NE-columns, that contain potential entity mentions, and LIT-columns, which house literal values. All text data is transformed to lower case, and special characters like underscores, along with extra spaces, are eliminated for cleaner, more uniform data.

2. **Lookup**: This step involves entity retrieval. For each entity mention, a set of candidates is derived via the LamAPI.

3. **Features Extraction**: For each candidate, a set of features is computed. These features, instrumental in identifying the correct candidate, can be categorized into three types: mention features, entity features, and mention-entity features. Some features focus solely on text similarity, while others consider the broader context.

4. **Initial Predictions**: An initial prediction is made using a machine learning model, resulting in a preliminary ranking of candidates.

5. **Features Extraction Revision**: The initial predictions provide some context. At this stage, we refine the features that consider the congruence of types and predicates collected from the candidates.

6. **Final Predictions**: New predictions are made using the refined types and predicate data from the previous step.

7. **Decision**: This is the final step where a decision is made for each mention about whether to annotate it or not. This is based on the confidence score and the difference between the top two candidates.

Through these steps, Alligator ensures a robust and accurate process for entity linking in your tabular data. It aims to facilitate the seamless integration of your data with Knowledge Graphs, enhancing the capabilities of your machine learning applications.


# SETUP

This section guides you through the setup process for the Alligator application. Please follow these steps carefully to ensure a successful setup.

## Preliminary Step: Install Docker and Docker-Compose

Before proceeding, ensure that Docker and Docker-Compose are installed on your system. These are essential for creating and managing the Alligator application's containers. 

Refer to the official Docker documentation for installation instructions: [Docker](https://docs.docker.com/get-docker/) and [Docker-Compose](https://docs.docker.com/compose/install/).

## Step 1: Environment Configuration

1. **.env File**: The application relies on environment variables specified in a `.env` file. An example template `.env-template` is provided in the repository. Please make sure to fill out the `.env` file correctly before proceeding. 

2. **LamAPI Dependency**: Alligator requires an instance of LamAPI. Ensure you have LamAPI up and running. For more information on setting up LamAPI, visit [LamAPI Repository](https://github.com/roby-avo/lamAPI).

## Step 2: Running the Application

1. **Starting the Application**: Navigate to the Alligator directory and execute the following command:
   
   ```bash
   docker-compose up

This command builds and starts the necessary containers for the Alligator application.

### Verifying the Setup
Once the application is running, you can access the SWAGGER UI interface at `http://localhost:<ALLIGATOR_PORT>`. Replace `<ALLIGATOR_PORT>` with the value you specified in the `.env` file for `ALLIGATOR_PORT`.

### Environment Variables Reference
Below is a brief description of the environment variables required in the `.env` file:

#### Alligator Configuration
- `ALLIGATOR_TOKEN`: Authentication token for Alligator.
- `ALLIGATOR_PORT`: The port on which the Alligator service will run.
- `MAX_NUMBER_OF_JOB`: Maximum number of concurrent jobs allowed.

#### MongoDB Configuration
- `MONGO_ENDPOINT`: MongoDB connection endpoint.
- `MONGO_INITDB_ROOT_USERNAME`: Root username for MongoDB.
- `MONGO_INITDB_ROOT_PASSWORD`: Root password for MongoDB.
- `MONGO_DBNAME`: Name of the MongoDB database.
- `MONGO_PORT`: Port for MongoDB connection.

#### Redis Configuration
- `REDIS_ENDPOINT`: Redis server connection endpoint.
- `REDIS_JOB_DB`: Redis database number for job data.

#### LAMAPI Configuration
- `LAMAPI_ENDPOINT`: Endpoint for the LamAPI service.
- `LAMAPI_TOKEN`: Authentication token for LamAPI.

#### Python Version Configuration
- `PYTHON_VERSION`: Python version to use.

#### MongoDB Version Configuration
- `MONGO_VERSION`: MongoDB version to use.

#### Configuration Values
- `CONFIG_VALUES`: Comma-separated configuration values in the order of DATASET_FOR_PAGE, TABLE_FOR_PAGE, CHUNK_SIZE (minimum number of rows for each process).

For a more detailed explanation of these variables, please refer to the `.env-template` file in the repository.

---

After completing these steps, your Alligator application should be set up and operational. For any issues or further configuration, refer to the respective sections in this documentation or the project's GitHub repository.
