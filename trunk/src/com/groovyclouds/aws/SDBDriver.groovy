/*
 * Copyright (c) 2009 GroovyClouds, Inc
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * GroovyClouds SimpleDB Driver
 */
package com.groovyclouds.aws

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import groovy.text.SimpleTemplateEngine

import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentHashMap

import com.groovyclouds.aws.exception.SDBException

class SDBDriver extends AWSConnection {
   /**
    * Note:
    * 1. Each method might return an exception. The exceptions are AWSException, ServiceException, and SDBException
    *
    * 2. Since AWS checks the validity of request parameters, the SDBDriver doesn't perform validity
    * checks of request parameters
    */

   public static final def MAX_NUM_DIGITS_KEY = "maxNumDigits"
   public static final def OFFSET_VALUE_KEY = "offsetValue"
   public static final def MAX_DIGITS_LEFT_KEY = "maxDigitsLeft"
   public static final def MAX_DIGITS_RIGHT_KEY = "maxDigitsRight"

   private static final def ITEM_NAME = "ItemName"
   private static final def MAX_QUERY_RESULT = 250

   private static final def DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ"

   private static final def SDB_URL = "http://sdb.amazonaws.com/"
   private static final def SDB_URL_SSL = "https://sdb.amazonaws.com/"

   private static final def MAX_NUM_DIGITS = 20
   private static final def OFFSET_VALUE = 1000000
   private static final def MAX_DIGITS_LEFT = 10
   private static final def MAX_DIGITS_RIGHT = 4

   private static final def INTEGER_TYPE = "!I"
   private static final def FLOATING_POINT_TYPE = "!F"
   private static final def BOOLEAN_TYPE = "!B"
   private static final def DATE_TYPE = "!D"

   private static ConcurrentHashMap SDB_Drivers = new ConcurrentHashMap()

   private static final Logger logger = LoggerFactory.getLogger(SDBDriver.class)

   private def sdbUrl

   private final def maxNumDigits
   private final def offsetValue
   private final def maxDigitsLeft
   private final def maxDigitsRight
   private final def zeroPadding

   private SDBDriver(awsAccessKeyID, awsSecretAccessKey, nameSpace = null, ConfigObject config = null) {
      super(awsAccessKeyID, awsSecretAccessKey, config?.aws?.sdb)

      this.useNameSpace = (config?.aws?.sdb?.getProperty(USE_NAMESPACE_KEY) == true)
      this.nameSpace = (nameSpace) ?: ""
      this.prefixNS = (useNameSpace && nameSpace) ? "${nameSpace}_" : ""

      sdbUrl = config?.aws?.sdb?.getProperty(USE_SSL_CONNECTION_KEY) == false ? SDB_URL : SDB_URL_SSL

      maxNumDigits = config?.aws?.sdb?.getProperty(MAX_NUM_DIGITS_KEY) ?: MAX_NUM_DIGITS
      offsetValue = config?.aws?.sdb?.getProperty(OFFSET_VALUE_KEY) ?: OFFSET_VALUE
      maxDigitsLeft = config?.aws?.sdb?.getProperty(MAX_DIGITS_LEFT_KEY) ?: MAX_DIGITS_LEFT
      maxDigitsRight = config?.aws?.sdb?.getProperty(MAX_DIGITS_RIGHT_KEY) ?: MAX_DIGITS_RIGHT

      initZeroPadding(maxNumDigits)
   }

   /**
    * @param awsAccessKeyID      AWS Access Key ID
    * @param awsSecretAccessKey  AWS Secret Access Key
    * @param nameSpace           user specified namespace, default null, no name required
    * @param                     config configuration object, default null to use default configuration
    * @return                    SDBDriver instance
    *
    * Notes:
    * This method needs to be called before the getInstance method for the nameSpace is called.
    * Format and default values of configuration object for SimpleDB
      aws {
          sdb {
             useSSLConnection = true
             useNameSpace = false

             maxNumDigits = 20
             offsetValue = 1000000
             maxDigitsLeft = 10
             maxDigitsRight = 4

             http {
                connectionTimeout = 60000
                socketTimeout = 60000
                maxConnectionsPerHost = 50
                staleCheckingEnabled = true
                tcpNoDelay = true
                httpProtocolExpectContinue = true
                numberRetries = 5
             }
         }
      }
    * When no namespace is specified, the instance is associated with the (accessKeyID, secretAccessKey) pair.
    * The key "_unnamed" is used to store this instance with no namespace and no prefixes are applied.
    * Once an instance is set up for a key (also namespace), it cannot be changed. However, there is no
    * restriction in creating as many instances with same (accessKeyID, secretAccessKey) pair.
    * A prefixNS is automatically prefixed to domain names when configuration 'useNameSpace = true'.
    * The prefixNS is transparent to users of the API and is an internal implementation detail that takes care of
    * stripping out the prefixNS to the API user.  So the API user will never have to deal with the prefixNS,
    * all calls to this API are made as if there is no such thing as a prefixNS.
    * Depending on the namespace context and the useNameSpace configuration, the prefixNS is added internally
    * in calls to SimpleDB and removed in return values.
    *
    * The only time the API user has to be aware of this is when the select statements are used.
    * The automatic prefixNS to domains feature is intended to achieve easy 'partitioning' of the SimpleDB domains
    * using the same (accessKeyID, secretAccessKey) pair.  This is particularly targeted for development mode where
    * multiple developers are working possibly using the same (accessKeyID, secretAccessKey) pair.
    */
   static setupInstance(awsAccessKeyID, awsSecretAccessKey, nameSpace = null, ConfigObject config = null) {
      def key = (nameSpace) ?: AWSConnection.UNNAMED_NAMESPACE
      def sdbDriver = SDB_Drivers.get(key)
      if (!sdbDriver) {
         SDB_Drivers.putIfAbsent(key,
                 new SDBDriver(awsAccessKeyID, awsSecretAccessKey, nameSpace, config))
         sdbDriver = SDB_Drivers.get(key)
      }
      else {
         def msg = """setupInstance method already called for SDB nameSpace:${nameSpace},
                            awsAccessKeyID:${awsAccessKeyID}, awsSecretAccessKey:${awsSecretAccessKey}
                        """.toString()
         logger.error(msg)
         throw new SDBException(msg)
      }
      return sdbDriver
   }

   /**
    * This method is provided to integrate easily with Java Properties in cases where this format is preferred
    * over the Groovy ConfigObject.
    *
    * @param awsAccessKeyID        AWS Access Key ID
    * @param awsSecretAccessKey    AWS Secret Access Key
    * @param nameSpace             user specified namespace for SimpleDB, default null, no name required
    * @param config                Java Properties object, default null to use default configuration
    *
    *  aws.sdb.useSSLConnection = true
       aws.sdb.useNameSpace = false
       aws.sdb.maxNumDigits = 20
       aws.sdb.offsetValue = 1000000
       aws.sdb.maxDigitsLeft = 10
       aws.sdb.maxDigitsRight = 4
       aws.sdb.http.connectionTimeout = 60000
       aws.sdb.http.socketTimeout = 60000
       aws.sdb.http.maxConnectionsPerHost = 50
       aws.sdb.http.staleCheckingEnabled = true
       aws.sdb.http.tcpNoDelay = true
       aws.sdb.http.httpProtocolExpectContinue = true
       aws.sdb.http.numberRetries = 5
    * @return                      SDBDriver instance
    */
   static setupInstance(awsAccessKeyID, awsSecretAccessKey, nameSpace=null, Properties config=null) {
      def configObject = getConfigObject(config)
      // Convert to correct data types
      setupConfigProps(configObject)
      return setupInstance(awsAccessKeyID, awsSecretAccessKey, nameSpace, configObject)
   }

   /**
    * Converts the properties to correct data types,
    * in addition to base properties, the following are SDB specific
    * maxNumDigits = 20
    * offsetValue = 1000000
    * maxDigitsLeft = 10
    * maxDigitsRight = 4
    */
   private static setupConfigProps(configObject) {
      if (!configObject) {
         return
      }

      AWSConnection.setupConfigProps(configObject.aws?.sdb)

      try {
         String nd = configObject.aws?.sdb?.getProperty(MAX_NUM_DIGITS_KEY)
         if (nd) {
            configObject.aws.sdb.setProperty(MAX_NUM_DIGITS_KEY, Integer.parseInt(nd))
         }
         String ov = configObject.aws?.sdb?.getProperty(OFFSET_VALUE_KEY)
         if (ov) {
            configObject.aws.sdb.setProperty(OFFSET_VALUE_KEY, Integer.parseInt(ov))
         }
         String ml = configObject.aws?.sdb?.getProperty(MAX_DIGITS_LEFT_KEY)
         if (ml) {
            configObject.aws.sdb.setProperty(MAX_DIGITS_LEFT_KEY, Integer.parseInt(ml))
         }
         String mr = configObject.aws?.sdb?.getProperty(MAX_DIGITS_RIGHT_KEY)
         if (mr) {
               configObject.aws.sdb.setProperty(MAX_DIGITS_RIGHT_KEY, Integer.parseInt(mr))
         }
      }
      catch (Exception e) {
         def msg = "SDB setupConfigProps Conversion Error"
         logger.error(msg)
         throw new SDBException(msg, e)
      }
   }

   /**
    * @param nameSpace     user specified name space, default null - no name required
    * @return SDBDriver    instance associated with the nameSpace
    *
    * Note: This method cannot be called for a nameSpace before the setupInstance method is called.
    */
   static getInstance(nameSpace = null) {
      def key = (nameSpace) ?: AWSConnection.UNNAMED_NAMESPACE
      def sdbdriver = SDB_Drivers.get(key)
      if (!sdbdriver) {
         def msg = "getInstance method called for SDB nameSpace:${nameSpace} before setupInstance method".toString()
         logger.error(msg)
         throw new IllegalStateException(msg)
      }
      return SDB_Drivers.get(key)
   }

   /**
    * Create a new domain
    *
    * @param  domainName   domain to be created in valid SimpleDB domain name format
    * @return              true if domain newly created
    *                      exception thrown if operation failed or domain already exists
    */
   def createDomain(domainName) {
      def domain = prefixNS + domainName
      if (domainExists(domainName)) {
         def err = "createDomain failed: ${domain} already exists".toString()
         logger.error(err)
         throw new SDBException(err)
      }

      def (statusCode, createDomainResponse) = doPostRequest(sdbUrl,
                                                             [["Action", "CreateDomain"],
                                                             ["DomainName", domain]])

      return parseResponse(createDomainResponse)
   }

   /**
    * Delete an existing domain
    *
    * @param domainName    domainName to be deleted
    * @return true         if domain is successfully deleted
    *                      exception thrown if operation failed or if the domain does not exist

    */
   def deleteDomain(domainName) {
      def domain = prefixNS + domainName
      if (!domainExists(domainName)) {
         def err = "deleteDomain failed: ${domain} does not exist".toString()
         logger.error(err)
         throw new SDBException(err)
      }

      def (statusCode, deleteDomainResponse) = doPostRequest(sdbUrl,
                                                             [["Action", "DeleteDomain"],
                                                             ["DomainName", domain]])

      return parseResponse(deleteDomainResponse)
   }

   /**
    * List all SimpleDB Domains returning a maximum of 100 domains.
    *
    * @return     list of domains if successful  [domain1, domain2, ...]
    *             empty list if there is no domain  [ ]
    *             exception thrown if operation failed
    */
   def listDomains() {
      def (statusCode, listDomainsResponse) = doPostRequest(sdbUrl,
                                                            [["Action", "ListDomains"]])

      logger.info("listDomains returned {} for prefix {}", listDomainsResponse, prefixNS)
      parseResponse(listDomainsResponse)

      def list = []
      def beginIndex = prefixNS.size ()
      def sdbDomain

      listDomainsResponse?.ListDomainsResult?.DomainName?.each {
         sdbDomain = it.toString ()
         if (sdbDomain.startsWith (prefixNS)) {
            list << sdbDomain [beginIndex..- 1]
         }
      }

      logger.info("list of domains {} for prefix {}", list, prefixNS)
      return list
   }

   /**
    * Check whether a domain exists.
    *
    * @param domainName    domain to be checked if it exists.
    * @return true         if domain exists
    *                      false otherwise
    *                      exception thrown if operation failed
    */
   def domainExists(domainName) {
      return listDomains()?.contains(domainName)
   }

   /**
    * Get Domain Metadata
    *
    * @param domainName    domain for which metadata is requested
    * @return Map          containing key-value pairs of metadata for the domain
    *                      exception thrown if operation failed
    */
   def getDomainMetadata(domainName) {
      def domain = prefixNS + domainName
      def (statusCode, domainMetadataResponse) = doPostRequest(sdbUrl,
                                                               [["Action", "DomainMetadata"],
                                                               ["DomainName", domain]])

      logger.info("getDomainMetadata returned {}", domainMetadataResponse)
      parseResponse(domainMetadataResponse)

      def domainMetadata = [:]
      domainMetadataResponse?.DomainMetadataResult?.children ()?.each {
         domainMetadata.put(it.name(), it.text())
      }

      logger.info("domainMetadata is {}", domainMetadata)
      return domainMetadata
   }

   /**
    * Create or Update an Item specifying it's unique identifier and a set of attributes in the specified domain.
    * A set of attributes associated with the unique identifier is created, an Item,  is created.
    * New attributes are created and for the Item's already existing attributes, it can be overwritten or
    * appended to existing values.
    *
    * @param domainName    name of domain (String)
    * @param itemName      unique identifier of the Item (String)
    * @param attributes    key-value pairs of attributes (Map)
    * @param doReplace     true or false, true will replace attributes, false will add to existing attribute value(s);
    *                      default true to replace
    * @return true         if successful
    *                      exception thrown if operation failed
    */
   def setItem(domainName, String itemName, attributes, doReplace = "false") {
      def domain = prefixNS + domainName
      logger.info("setItem domainName:{}, itemName:{}, attributes:{}, doReplace:",
              domain, itemName, attributes, doReplace)
      return putAttributes(domain, itemName, attributes, doReplace)
   }

   /**
    * Batch Create or Update Items
    *
    * @param domainName    name of domain (String)
    * @param itemMap       Map of Items (ItemName is key with attributes Map as value)
    * @param doReplace     true or false, true will replace attributes, false will add to existing attribute value(s);
    *                      default true to replace
    * @return true         if successful
    *                      exception thrown if operation failed
    */
   def setItem(domainName, itemMap, doReplace = "false") {
      def domain = prefixNS + domainName
      logger.info("setItem domainName:{}, itemMap:{}, doReplace:", domain, itemMap, doReplace)
      return batchPutAttributes(domain, itemMap, doReplace)
   }

   /**
    * Delete one or more attributes of an Item.  Specifying no attributes will delete all the attributes as well as the
    * Item itself.
    *
    * @param domainName    name of domain (String)
    * @param itemName      unique identifier of the Item (String)
    * @param attributes    attributes to delete; if empty or null, the Item is deleted (List)
    *                      default value is null, will delete all attributes of the Item, and hence the Item.
    * @return true         if successful
    *                      exception thrown if operation failed
    */
   def deleteItem(domainName, String itemName, attributes = null) {
      def domain = prefixNS + domainName
      logger.info("deleteItem domainName:{}, itemName:{}, attributes:{}",
                  domain, itemName, attributes)
      return deleteAttributes(domain, itemName, attributes)
   }

   /**
    * Get one or more attributes of an Item.  Specifying no attributes will get all the attributes.
    * ItemName key stores the unique identifier of the Item.
    *
    * @param domainName    name of domain (String)
    * @param itemName      unique identifier of the Item (String)
    * @param attributes    attributes to get; if empty or null, all attributes of Item is retrieved (List)
    *                      default value is null
    * @return Map          of attributes
    *                      exception thrown if operation failed
    */
   def getItem(domainName, String itemName, attributes = null) {
      def domain = prefixNS + domainName
      logger.info("getItem domainName:{}, itemName:{}, attributes:{}", domain, itemName, attributes)
      return getAttributes(domain, itemName, attributes)
   }

   /**
    * Builds a dynamic query expression from a template string with variables by replacing the values
    * for the variables.  This takes care of the encoding required for data type values in the query.
    * Note that domain names are not automatically prefixed, the caller needs to take care of this explicitly.
    *
    * @param queryExpr           Template for query expression including variables to be replaced
    * @param queryBinding        Key-Value pairs of variables and values to be replaced in the template
    * @return Query Epxression   with variables replaced (String)
    *                            exception thrown if operation failed
    */
   def buildQueryExpression(queryExpr, queryBinding) {
      def qExpr = queryExpr
      if (queryBinding) {
         def binding = [:]
         try {
            def engine = new SimpleTemplateEngine()
            def instance = engine.createTemplate(queryExpr)
            queryBinding?.each {k, v ->
               binding.put(k, encodeString(v))
            }
            qExpr = instance.make(binding).toString()
         }
         catch (Exception e) {
            def err = "Couldn't build queryExpr: ${queryExpr}, binding: ${queryBinding}".toString()
            logger.error(err, e)
            throw new SDBException(err, e)
         }
      }
      return qExpr
   }

   /**
    * Query the domain using the query expression and optionally a cursor to get matching Items.
    * Use the buildQueryExpression to ensure data type encoding is taken care of in the query expression.
    * Returns a list of ItemNames for the matched Items and a NextToken to fetch more if there are more.
    *
    * @param domainName       domain from which to select theItems
    * @param queryExpression  SDB query expression
    * @param nextToken        optional cursor, default null
    * @param maxQueryResult   range from 1 to 250. If not specified or null, then set to MAX_QUERY_RESULT
    * @return List of ItemNames of matched Items and NextToken (String) if there are more matches
    *                         exception thrown if operation failed
    */
   def query(domainName, queryExpression, String nextToken = null, int maxQueryResult = MAX_QUERY_RESULT) {
      def domain = prefixNS + domainName
      logger.info("Query domainName: {}, queryExpression {}, nextToken {}, maxQueryResult {}",
                  domain, queryExpression, nextToken, maxQueryResult)
      def queryExpr = [["QueryExpression", queryExpression]]
      if (nextToken) {
         queryExpr += [["NextToken", nextToken]]
      }
      return doQuery(domain, queryExpr, maxQueryResult)
   }

   /**
    * Query the domain with a query expression to get matching Items and the specified attributes.
    * Specify the NextToken to get the next list of Items.
    * Use the buildQueryExpression to ensure data type encoding is taken care of in the query expression
    *
    * @param domainName       domain to query for Items
    * @param queryExpression  SDB query expression
    * @param nextToken        optional cursor
    * @param attributes       List of attributes to retrieve
    * @param maxQueryResult   range from 1 to 250. If not specified or null, then set to MAX_QUERY_RESULT
    * @return List of Items, each Item is a Map of attributeName-attributeValue pairs
    *                          and the NextToken (String) if there are more matches
    *                         exception thrown if operation failed
    */
   def queryWithAttributes(domainName, queryExpression, List attributes = null, String nextToken = null,
                           int maxQueryResult = MAX_QUERY_RESULT) {
      def domain = prefixNS + domainName
      logger.info("QueryWithAttributes domainName: {}, queryExpression {}, nextToken {}, attributes {}, maxQueryResult {}",
              domain, queryExpression, nextToken, attributes, maxQueryResult)
      def queryExpr = [["QueryExpression", queryExpression]]
      if (nextToken) {
         queryExpr += [["NextToken", nextToken]]
      }
      return doQueryWithAttributes(domain, queryExpr, attributes, maxQueryResult)
   }

   /**
    * Select the Items that match the select query expression.
    * Note that the buildQueryExpression method is to be used to build the expression correctly to ensure internal
    * data type encoding is done.  Also the domain name used in the expressions need to be specified taking into
    * account that the schemaPrefix maybe used internally.
    *
    * @param selectExpression    SDB select expression
    * @param nextToken           optional cursor
    * @return List of Items, each Item is a Map of attributeName-attributeValue pairs
    *                          and the NextToken (String) if there are more matches
    *                            exception thrown if operation failed
    */
   def select(selectExpression, nextToken = null) {
      def selectExpr = [["SelectExpression", selectExpression]]
      if (nextToken) {
         selectExpr += [["NextToken", nextToken]]
      }

      return doSelect(selectExpr)
   }

   /**
    * Add/Replace an Item attributes with a unique ItemName in a given domain.
    * If an attribute value is numeric/boolean/date, it is first converted into an internal
    * string format before storing into SDB. This is to allow for query manipulation.
    *
    *
    * @param domainName domain where the Item is to be stored
    * @param itemName unique identifier of the Item
    * @param attributes Map of attributes/values in the following format:
    *                      [attname1: value1, attname2, value2, ..., attnamen, valuen]
    *                      value is either a single element or a list (multi-valued attribute)
    *                      Example: [ color : ["blue", "red"], size: [10, 10.67], name: 'spring collection']
    * @return true      if operation is successful
    *                   exception thrown if operation failed
    */
   private def putAttributes(domainName, String itemName, attributes, doReplace) {
      def (statusCode, response) = doPostRequest(sdbUrl,
                                                 [["Action", "PutAttributes"],
                                                  ["DomainName", domainName],
                                                  ["ItemName", itemName]] +
                                                  buildPutAttributes(attributes, doReplace))
      return parseResponse(response)
   }

   private def batchPutAttributes(domainName, itemMap, doReplace) {
      def (statusCode, response) = doPostRequest(sdbUrl,
                                                 [["Action", "BatchPutAttributes"],
                                                 ["DomainName", domainName]] +
                                                 buildBatchPutAttributes(itemMap, doReplace))
      return parseResponse(response)
   }

   /**
    * Given a domain, the unique identifer item name, and optional list of attributes, get values for the attributes.
    * If attributes is not specified, then all of the attribute values are retrieved.
    * All internal string formatted values are converted into typed values.
    *
    * @param domainName domain where the item is located
    * @param itemName unique identifier ItemName
    * @param attributes list of attributes to retrieve; default is null
    * @return Map of attributes/values in the following format:
    *                          [attname1: value1, attname2, value2, ..., attnamen, valuen] if operation is successful
    */
   private def getAttributes(domainName, String itemName, attributes = null) {
      def (statusCode, getAttributesResponse) = doPostRequest(sdbUrl,
                                                              [["Action", "GetAttributes"]] +
                                                              buildAttributes (attributes, "get") +
                                                              [
                                                              ["DomainName", domainName],
                                                              ["ItemName", itemName]
                                                              ])
      parseResponse(getAttributesResponse)
      return formatItemAttributes(itemName, getAttributesResponse?.GetAttributesResult?.Attribute)
   }

   /**
    * Given a domain, an item name, and a list of attributes, it removes the attributes from the item.
    * If the item has no more attributes left, the method removes the item from the domain.
    * If attributes is empty, then the entire Item is deleted
    *
    * @param domainName domain where the item is stored
    * @param itemName unique identifier ItemName
    * @return true if operation is successful
    *                      false otherwise
    *                      If the item does not exist, the operation still returns true
    */
   private def deleteAttributes(domainName, String itemName, attributes = null) {
      def (statusCode, deleteAttributesResponse) = doPostRequest (sdbUrl,
                                                                  [["Action", "DeleteAttributes"]] +
                                                                  buildAttributes (attributes, "delete") +
                                                                  [
                                                                  ["DomainName", domainName],
                                                                  ["ItemName", itemName]
                                                                  ])
      return parseResponse(deleteAttributesResponse)
   }

   /**
    * Given a domain, an SDB query expression or a cursor ((NextToken), returns the list of matching items.
    *
    * @param domainName domain where items are located
    * @param queryExpressionOrNextToken SDB query expression or a cursor (NextToken)
    * @param maxQueryResult range from 1 to 250. If set to null, then value is MAX_QUERY_RESULT
    * @return List of item names and NextToken
    */
   private def doQuery(domainName, queryExpressionOrNextToken, maxQueryResult) {
      String MaxNumberOfItems = (maxQueryResult && maxQueryResult < MAX_QUERY_RESULT) ?
         maxQueryResult.toString() : MAX_QUERY_RESULT.toString()

      def call = [["Action", "Query"],
              ["DomainName", domainName],
              ["MaxNumberOfItems", MaxNumberOfItems]] +
              queryExpressionOrNextToken

      def (statusCode, queryItemResponse) = doPostRequest(sdbUrl, call)
      parseResponse(queryItemResponse)

      def list = []
      queryItemResponse?.QueryResult?.ItemName?.each {
         list << it.toString ()
      }

      def nextToken = queryItemResponse?.QueryResult?.NextToken?.toString()
      logger.info("doQuery returned result: {}, nextToken: {}", list, nextToken)
      return [list, nextToken]
   }


   /**
    * Given a domain, SDB query expression or a cursor (NextToken), returns list of matching items and it's attributes.
    *
    * @param domainName domain where items are located
    * @param queryExpressionOrNextToken SDB query expression or a cursor (NextToken)
    * @param attributes set of attributes to fetch with matching item
    * @param maxQueryResult range from 1 to 250. If set to null, then set to MAX_QUERY_RESULT
    * @return List of item attributes as key-value map, NextToken
    */
   private def doQueryWithAttributes(domainName, queryExpressionOrNextToken, attributes, maxQueryResult) {
      String MaxNumberOfItems = (maxQueryResult && maxQueryResult < MAX_QUERY_RESULT) ?
         maxQueryResult.toString() : MAX_QUERY_RESULT.toString()

      def call = [["Action", "QueryWithAttributes"]]
      if (attributes) {
         call += buildAttributes(attributes, "get")
      }
      call += [["DomainName", domainName],
              ["MaxNumberOfItems", MaxNumberOfItems]] +
              queryExpressionOrNextToken
      def (statusCode, queryItemResponse) = doPostRequest(sdbUrl, call)
      parseResponse(queryItemResponse)

      def list = []

      queryItemResponse?.QueryWithAttributesResult?.Item?.each {
         def queryItem = formatItemAttributes(it.Name, it.Attribute)
         list << queryItem
      }

      def nextToken = queryItemResponse?.QueryWithAttributesResult?.NextToken?.toString()
      logger.info("doQueryWithAttributes returned result: {}, nextToken: {}", list, nextToken)
      return [list, nextToken]
   }

   /**
    * Select operation returning Items that match the select criteria.
    *
    * @param selectExpr Select expression
    * @return List of Items (Map of attribute name-value), NextToken
    */
   private def doSelect(selectExpr) {
      def call = [["Action", "Select"]] + selectExpr
      def (statusCode, selectResponse) = doPostRequest(sdbUrl, call)
      parseResponse(selectResponse)
      def items = []
   
      selectResponse?.SelectResult?.Item?.each {
         item ->
         def selectedItem = [:]
         items << formatItemAttributes(item.Name, item.Attribute)
      }

      def nextToken = selectResponse?.NextToken?.toString()
      logger.info("Select Expression:{},  Items: {}, nextToken: {}", selectExpr, items, nextToken)
      return [items, nextToken]
   }

   /**
    * Given a list of attribute name-value and doReplace flag, build the attribute/value/flag as per
    * SDB requirements for "put" action. The doReplace flag is either true or false; true will overwrite existing
    * attributes; false will add new value to existing attribute (multi-valued attribute)
    *
    * If an attribute value is numeric/boolean/date, it is first converted into an internal
    * string format before storing into SDB. This is to allow for query manipulation.
    *
    * @param attributes Map of attribute name-value, value being single valued or a list (multi-valued)
    * @param doReplace true or false
    * @return List of the parameter-value pairs for attribute name, value and replace in SDB format
    */
   private def buildPutAttributes(attributes, doReplace) {
      def attribute = []
      def i = 0
      def istr

      logger.debug("buildPutAttributes - attributes: {}, doReplace: {}", attributes, doReplace)
      attributes.each {
         key, value ->

         logger.debug("buildPutAttributes - attribute key: {}, value: {}", key, value)
         if (!(value instanceof List)) {
            value = [value]
         }

         value.each {
            istr = i
            attribute += [["Attribute.${istr}.Name", key]]
            logger.debug("buildPutAttributes - attribute : {}", attribute)
            if (doReplace == "true") attribute += [["Attribute.${istr}.Replace", doReplace]]
            logger.debug("buildPutAttributes - after doReplace, attribute : {}", attribute)
            logger.debug("buildPutAttributes - before encodeToString, value : {}", it)
            it = encodeString(it)
            logger.debug("buildPutAttributes - after encodeToString, value : {}", it)
            attribute += [["Attribute.${istr}.Value", it]]
            logger.debug("buildPutAttributes - attribute : {}", attribute)
            i++
         }
      }

      return attribute
   }

   /**
    * Given a Map of itemName key and Map of attribute name-value pairs and doReplace flag,
    * build the attribute/value/flag as per
    * SDB requirements for "put" action. The doReplace flag is either true or false;
    * true will overwrite existing attributes;
    * false will add new value to existing attribute (multi-valued attribute)
    *
    * If an attribute value is numeric/boolean/date, it is first converted into an internal
    * string format before storing into SDB. This is to allow for query manipulation.
    *
    * @param itemMap Map of Key=ItemName and Value=map of attribute name-value pair,
    *                          value being single valued or a list (multi-valued)
    * @param doReplace true or false
    * @return List of ItemName, parameter-value pairs for attribute name, value and replace in SDB format
    */
   private def buildBatchPutAttributes(itemMap, doReplace = "false") {
      def attribute = []
      def itemIndex = 0
      def i = 0
      def istr

      logger.debug("buildBatchPutAttributes - itemMap: {}, doReplace: {}", itemMap, doReplace)
      itemMap.each {itemName, attributes ->
         attribute += [["Item.${itemIndex}.ItemName", itemName]]
         logger.debug("buildBatchPutAttributes - add ItemName, attribute : {}", attribute)
         attributes.each {
            key, value ->

            logger.debug("buildBatchPutAttributes - attribute key: {}, value: {}", key, value)
            if (!(value instanceof List)) {
               value = [value]
            }

            value.each {
               istr = i
               attribute += [["Item.${itemIndex}.Attribute.${istr}.Name", key]]
               logger.debug("buildBatchPutAttributes - attribute : {}", attribute)
               if (doReplace == "true") attribute += [["Item.${itemIndex}.Attribute.${istr}.Replace", doReplace]]
               logger.debug("buildBatchPutAttributes - after doReplace, attribute : {}", attribute)
               logger.debug("buildBatchPutAttributes - before encodeToString, value : {}", it)
               it = encodeString(it)
               logger.debug("buildBatchPutAttributes - after encodeToString, value : {}", it)
               attribute += [["Item.${itemIndex}.Attribute.${istr}.Value", it]]
               logger.debug("buildBatchPutAttributes - attribute : {}", attribute)
               i++
            }
         }
         itemIndex++
      }
      return attribute
   }

   /**
    * Given a list of attributes, build the attribute name list as per SDB requirements for "delete" and "get" action.
    *
    * @param attributes list of attributes, null or empty is allowed
    * @return SDB formatted for attribute name list "delete" or "get" action
    */
   private def buildAttributes(attributes, operation) {
      logger.debug("buildAttributes - attribute: {}, operation: {}", attributes, operation)
      def attribute = []
      def i = 0
      def istr
      attributes?.each
      {
         it ->
         istr = i
         if (operation == "delete") {
            attribute += [["Attribute.${istr}.Name", it]]
         }
         else if (operation == "get") {
            attribute += [["AttributeName.${istr}", it]]
         }
         logger.debug("buildAttributes - i: {}, attribute: {}, attributes: {}", i, it, attribute)
         i++
      }

      return attribute
   }

   /**
    * Given an SDB item list response, constructs a map of attributes/values by decoding data types and handling single
    * and multi-valued attributes.
    *
    * @param itemName SDB item response format (XML format)
    * @return Map of attribute name-value with multi-valued attributes in a list
    */
   private def formatItemAttributes(itemName, itemAttributes) {
      def attribute = [:]
      def size = itemAttributes?.size()
      def value, valueFormatted, valueStr
      def keyStr
      if (size > 0) {
         (0..size - 1).each
         {
            valueStr = itemAttributes.Value[it].toString()
            valueFormatted = decodeString(valueStr)
            keyStr = itemAttributes.Name[it].toString()
            logger.debug("formatItemAttributes - Key: {}, Value: {}, Formatted Value: {}", keyStr, valueStr, valueFormatted)
            value = attribute.get(keyStr)

            if (value) {
               if (!(value instanceof List)) {
                  value = [value]
               }
               value += [valueFormatted]
            }
            else {
               value = valueFormatted
            }
            attribute += [(keyStr): value]
            logger.debug("formatItemAttributes - key: {} -> value: {}", keyStr, value)
         }
         attribute += [(ITEM_NAME): itemName.toString()]
      }
      logger.debug("formatItemAttributes - attribute: {}", attribute)

      return attribute
   }

   /**
    * Checks for valid response from the server.
    * If any error is detected, then throw SDBException for client to handle it.
    *
    * @param response XML response from server
    * @return true if valid and no error detected
    *                      throws SDBException on error response
    */
   private def parseResponse(response) throws SDBException {
      def valid = (!(response.Errors?.Error?.Code?.text()))
      parseResponseMetadata(response.ResponseMetadata)
      def errMsg, msg
      response?.Errors?.Error?.each {
         if (!errMsg) {
            errMsg = new StringBuilder()
         }
         def err = "Error Code: ${it.Code.text()},  Message: ${it.Message}"
         errMsg.append(err)
      }

      if ((msg = errMsg?.toString())) {
         logger.error(msg)
         throw new SDBException(msg)
      }
      return valid
   }

   /**
    * Log the RequestID and BoxUsage from the server at info level.
    *
    * @param responseMetadata XML response from the server
    */
   private def parseResponseMetadata(responseMetadata) {
      def requestId = responseMetadata.RequestId.toString()
      def usage = responseMetadata.BoxUsage.toString()
      logger.info("RequestID: {}, BoxUsage: {}", requestId, usage)
   }

   private def initZeroPadding(maxNumDigits) {
      StringBuilder strbuf = new StringBuilder(maxNumDigits)
      (1..maxNumDigits).each
      {
         strbuf << '0'
      }
      this.zeroPadding = strbuf.toString()
   }

   private String encodeString(st) {
      String encodedSt

      if (st instanceof Integer) {
         encodedSt = encodeRealNumberRange(st, maxNumDigits, offsetValue)
      }
      else if (st instanceof BigDecimal) {
         encodedSt = encodeRealNumberRange(st, maxDigitsLeft, maxDigitsRight, offsetValue)
      }
      else if (st instanceof Boolean) {
         encodedSt = encodeBoolean(st)
      }
      else if (st instanceof Date) {
         encodedSt = encodeDate(st)
      }
      else {
         encodedSt = encodeText(st)
      }

      return encodedSt
   }

   private def decodeString(st) {
      def value

      if (isIntegerEncoded(st)) {
         value = decodeRealNumberRange(st, offsetValue)
      }
      else if (isFloatEncoded(st)) {
         value = decodeRealNumberRange(st, maxDigitsRight, offsetValue)
      }
      else if (isBooleanEncoded(st)) {
         value = decodeBoolean(st)
      }
      else if (isDateEncoded(st)) {
         value = decodeDate(st)
      }
      else {
         value = decodeText(st)
      }

      return value
   }

   /**
    * Encodes real integer value into a string by offsetting and zero-padding
    * number up to the specified number of digits.
    *
    * @param number Integer to be encoded
    * @param maxNumDigits maximum number of digits in the largest absolute value in the data set
    * @param offsetValue offset value, has to be greater than absolute value of any negative number in the data set
    * @return string representation of the encoded integer
    */
   private String encodeRealNumberRange(Integer number, maxNumDigits, offsetValue) {
      def offsetNumber = number + offsetValue
      String offsetString = offsetNumber.toString()
      def numZeroes = maxNumDigits - offsetString.length()
      StringBuilder strBuffer = new StringBuilder(INTEGER_TYPE.size() + numZeroes + offsetString.length())
      strBuffer << INTEGER_TYPE
      strBuffer << zeroPadding[0..numZeroes - 1]
      strBuffer << offsetString
      return strBuffer.toString()
   }

   /**
    * Encodes BigDecimal number into a string by offsetting and zero-padding the number
    * up to the specified number of digits.
    *
    * @param number BigDecimal to be encoded
    * @param maxDigitsLeft maximum number of digits left of the decimal point in the largest
    *                        absolute value in the data set
    * @param maxDigitsRight maximum number of digits right of the decimal point in the largest absolute value
    in the data set, i.e. precision
    * @param offsetValue offset value, has to be greater than absolute value of any negative number
    in the data set.
    * @return string representation of the encoded number
    */
   private String encodeRealNumberRange(BigDecimal number, maxDigitsLeft, maxDigitsRight, offsetValue) {
      int shiftMultiplier = (int) Math.pow(10, maxDigitsRight)
      long shiftedNumber = (long) Math.round(number * shiftMultiplier)
      def shiftedOffset = offsetValue * shiftMultiplier
      def offsetNumber = shiftedNumber + shiftedOffset
      String offsetString = offsetNumber.toString()
      def numBeforeDecimal = offsetString.length()
      def numZeroes = maxDigitsLeft + maxDigitsRight - numBeforeDecimal
      StringBuilder strBuffer = new StringBuilder(FLOATING_POINT_TYPE.size() + numZeroes + offsetString.length())
      strBuffer << FLOATING_POINT_TYPE
      strBuffer << zeroPadding[0..numZeroes - 1]
      strBuffer << offsetString
      return strBuffer.toString()
   }

   /**
    * Decodes integer value from the string representation that was created by
    * using encodeRealNumberRange(..) function.
    *
    * @param value string representation of the integer value
    * @param offsetValue offset value that was used in the original encoding
    * @return original integer value
    */
   private int decodeRealNumberRange(String value, offsetValue) {
      def start = INTEGER_TYPE.size()
      value = value[start..-1]
      long offsetNumber = Long.parseLong(value, 10)
      return (int) (offsetNumber - offsetValue)
   }

   /**
    * Decodes float value from the string representation that was created by using
    * encodeRealNumberRange(..) function.
    *
    * @param value string representation of the integer value
    * @param maxDigitsRight maximum number of digits left of the decimal point in
    * the largest absolute value in the data set (must be the same as the one used for encoding).
    * @param offsetValue offset value that was used in the original encoding
    * @return original float value
    */
   private BigDecimal decodeRealNumberRange(String value, maxDigitsRight, offsetValue) {
      def start = FLOATING_POINT_TYPE.size()
      value = value[start..-1]
      long offsetNumber = Long.parseLong(value, 10)
      int shiftMultiplier = (int) Math.pow(10, maxDigitsRight)
      double tempVal = (double) (offsetNumber - offsetValue * shiftMultiplier)
      return (BigDecimal) (tempVal / (double) (shiftMultiplier))
   }

   /**
    * Encodes date value into string format that can be compared lexicographically
    *
    * @param date date value to be encoded
    * @return string representation of the date value
    */
   private String encodeDate(Date date) {
      SimpleDateFormat dateFormatter = new SimpleDateFormat(DATE_FORMAT)
      /* Java doesn't handle ISO8601 nicely: need to add ':' manually */
      String result = dateFormatter.format(date)
      return DATE_TYPE + result.substring(0, result.length() - 2) + ":" +
              result.substring(result.length() - 2)
   }

   /**
    * Decodes date value from the string representation created using encodeDate(..) function.
    *
    * @param value string representation of the date value
    * @return original date value
    */
   private Date decodeDate(String value) {
      def start = DATE_TYPE.size()
      value = value[start..-1]
      String javaValue = value.substring(0, value.length() - 3) + value.substring(value.length() - 2)
      SimpleDateFormat dateFormatter = new SimpleDateFormat(DATE_FORMAT)
      return dateFormatter.parse(javaValue)
   }

   /**
    * Encodes boolean value into string format that can be compared lexicographically
    *
    * @param boolean date value to be encoded
    * @return string representation of the boolean value
    */
   private String encodeBoolean(Boolean b) {
      if (b) {
         return BOOLEAN_TYPE + "true"
      }
      else {
         return BOOLEAN_TYPE + "false"
      }
   }

   /**
    * Decodes boolean value from the string representation created using encodeBoolean(..) function.
    *
    * @param value string representation of the boolean value
    * @return original boolean value
    */
   private Boolean decodeBoolean(String value) {
      def start = BOOLEAN_TYPE.size()
      value = value[start..-1]
      return (value == "true")
   }

   private String encodeText(String value) {
      if (!value) {
         return ""
      }
      def str = value.replaceAll("<", "&lt;")
      str = str.replaceAll(">", "&gt;")
      str = str.replaceAll("'", "&apos;")
      str = str.replaceAll("\"", "&quot;")
      str = str.replaceAll("&", "&amp;")
      return str
   }

   private String decodeText(String value) {
      if (!value) {
         return ""
      }
      def str = value.replaceAll("&lt;", "<",)
      str = str.replaceAll("&gt;", ">")
      str = str.replaceAll("&apos;", "'")
      str = str.replaceAll("&quot;", "\"")
      str = str.replaceAll("&amp;", "&")
      return str
   }

   private def isIntegerEncoded(String value) {
      return (value.startsWith(INTEGER_TYPE))
   }

   private def isFloatEncoded(String value) {
      return (value.startsWith(FLOATING_POINT_TYPE))
   }

   private def isDateEncoded(String value) {
      return (value.startsWith(DATE_TYPE))
   }

   private def isBooleanEncoded(String value) {
      return (value.startsWith(BOOLEAN_TYPE))
   }
}