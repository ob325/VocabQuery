##' Query the concept table 
##'
##' @title queryConceptName
##' @param connectionDetails ConnectionDetails object
##' @param schema database schema, character 
##' @param pattern regex pattern to match 
##' @param domain filter to value of OMOP domain_id
##' @param standard filter to value of OMOP standard_concept
##' @param class fiter to value of OMOP class_id
##' @param vocab filter to value of OMOP vocabulary_id
##' @param validity fiter to value of OMOP invalid_reason
##' @param cols character vector of columns to include in result 
##' @return tibble 
##' @export
queryConceptName <- function(connectionDetails,
                             schema,
                             pattern,
                             domain = NULL,
                             standard = NULL,
                             class = NULL,
                             vocab = NULL,
                             validity = NULL,
                             cols = c("concept_id", "concept_name",
                                      "vocabulary_id", "standard_concept",
                                      "domain_id", "vocabulary_id",
                                      "concept_class_id")) {
  
  checkmate::assertClass(connectionDetails,
                         "ConnectionDetails")
  checkmate::assertCharacter(schema, min.chars = 1) 
  checkmate::assertCharacter(pattern, min.chars = 1) 

  connection <- DatabaseConnector::connect(
    connectionDetails)

  on.exit(DatabaseConnector::disconnect(connection))

  colsSql <- paste0(cols, collapse = ", ")

  additionalWhere <- mapply(\(a, b) ifelse(!is.null(a),
                                        paste0("and ", b, " = '", a, "'"),
                                        ""),
                            list(domain, concept,
                                 class, vocab,
                                 validity),
                            list("domain_id", "standard_concept",
                                 "concept_class_id", "vocabulary_id",
                                 "validity")
                            ) 
  additionalWhere <- paste0(additionalWhere, collapse = " ")
  
  result <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "select @colsSql
     from @schema.concept
     where concept_name like '@pattern'
       @additionalWhere", 
    colsSql = colsSql,
    schema = schema,
    pattern = pattern,
    additionalWhere = additionalWhere) 

  return(result)
}


##' query distinct values of a column in concept table 
##'
##' 
##' @title conceptTableDistinctValues 
##' @param connectionDetails 
##' @param schema 
##' @param column 
##' @param resultsLimit 
##' @return character
##' @author William J. O'Brien
##' @export
conceptTableDistinctValues <- function(connectionDetails,
                                schema,
                                column,
                                resultsLimit = 100) {

  checkmate::assertClass(connectionDetails,
                         "ConnectionDetails")
  checkmate::assertCharacter(schema, min.chars = 1)

  stopifnot(column %in% c("domain_id", "vocabulary_id",
                          "concept_class_id", "standard_concept"))
                          
  connection <- DatabaseConnector::connect(
    connectionDetails)

  on.exit(DatabaseConnector::disconnect(connection))
  
  result <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "select distinct @column
     from @schema.concept
     order by @column",
    column = column,
    schema = schema)

  return(result) 
}


##' descendants
##'
##' Get descendants of a standard concept
##' @title descendants 
##' @param connectionDetails 
##' @param conceptId 
##' @return tibble
##' @export 
descendants <- function(connectionDetails,
                        schema,
                        conceptId) {
  
  checkmate::assertClass(connectionDetails,
                         "ConnectionDetails")
  checkmate::assertIntegerish(conceptId)
  checkmate::assertCharacter(schema) 
                          
  connection <- DatabaseConnector::connect(
    connectionDetails
  )

  on.exit(DatabaseConnector::disconnect(connection))
  
  result <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "select a.ancestor_concept_id,
       ca.concept_name ancestor_concept_name,
       a.descendant_concept_id,
       cb.concept_name descendant_concept_name 
     from @schema.concept_ancestor a
       left join @schema.concept ca
         on a.ancestor_concept_id = ca.concept_id
       left join @schema.concept cb
         on a.descendant_concept_id = cb.concept_id
     where ancestor_concept_id = @conceptId",
    schema = schema,
    conceptId = conceptId)
  
  return(result) 
}
