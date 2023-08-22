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

