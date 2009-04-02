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
 * GroovyClouds SQS Driver
 */

package com.groovyclouds.aws

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap

import com.groovyclouds.aws.exception.SQSException

class SQSDriver extends AWSConnection {
   /**
    * Note:
    * 1. Each method might return an exception. The exceptions are AWSException, ServiceException, and SQSException
    *
    * 2. AWS queue URL returned in the XML by AWS is always in http format even if we use https to make
    *    the request. This is why we always construct the AWS queue URL for each request
    *
    * 3. Since AWS checks the validity of request parameters (such as queue name,
    *    DefaultVisibilityTimeout, MaxNumberOfMessages, QueueNamePrefix, MessageBody, etc.),
    *    the SQSDriver doesn't perform validity checks of request parameters
    */
   public static final def DEFAULT_VISIBILITY_TIMEOUT_KEY = "defaultVisibilityTimeout"
   public static final def DEFAULT_MAX_NUMBER_OF_MESSAGES_KEY = "defaultMaxNumberOfMessages"

   private static final def SQS_URL = "http://queue.amazonaws.com/"
   private static final def SQS_URL_SSL = "https://queue.amazonaws.com/"

   private static final def DEFAULT_VISIBILITY_TIMEOUT = 30
   private static final def DEFAULT_MAX_NUMBER_OF_MESSAGES = 1

   private static final def SDS_VERSION_2008 = "2008-01-01"

   private static ConcurrentHashMap SQS_Drivers = new ConcurrentHashMap()

   private static final Logger logger = LoggerFactory.getLogger(SQSDriver.class)

   private def sqsUrl
   private def defaultVisibilityTimeout
   private def defaultMaxNumberOfMessages

   private SQSDriver(awsAccessKeyID, awsSecretAccessKey, nameSpace = null, ConfigObject config = null) {
      super(awsAccessKeyID, awsSecretAccessKey, config?.aws?.sqs)

      this.useNameSpace = (config?.aws?.sqs?.getProperty(USE_NAMESPACE_KEY) == true)
      this.nameSpace = (nameSpace) ?: ""
      this.prefixNS = (useNameSpace && nameSpace) ? "${nameSpace}_" : ""
      this.sdsVersion = SDS_VERSION_2008

      sqsUrl = config?.aws?.sqs?.getProperty(USE_SSL_CONNECTION_KEY) == false ? SQS_URL : SQS_URL_SSL

      defaultVisibilityTimeout = config?.aws?.sqs?.getProperty(DEFAULT_VISIBILITY_TIMEOUT_KEY) ?: DEFAULT_VISIBILITY_TIMEOUT
      defaultMaxNumberOfMessages = config?.aws?.sqs?.getProperty(DEFAULT_MAX_NUMBER_OF_MESSAGES_KEY) ?: DEFAULT_MAX_NUMBER_OF_MESSAGES
   }

   /**
    * @param awsAccessKeyID      AWS Access Key ID
    * @param awsSecretAccessKey  AWS Secret Access Key
    * @param nameSpace           user specified namespace, default null, no name required
    * @param config              configuration object, default null to use default configuration
    * @return                    SQSDriver instance; exception thrown if operation failed
    *
    * Notes:
    * This method needs to be called before the getInstance method for the nameSpace is called.
    * Format and default values of configuration object for SQS
      aws {
        sqs {
          useSSLConnection = true
          useNameSpace = false

          defaultVisibilityTimeout = 30
          defaultMaxNumberOfMessages = 1

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
    * Once an instance is set up for a key (also namespace), it cannot be changed.  However, there is no restriction
    * in creating as many instances with same (accessKeyID, secretAccessKey) pair.
    * A prefixNS is automatically prefixed to queue names when configuration 'useNameSpace = true'.
    * The prefixNS is transparent to users of the API and is an internal implementation detail that takes care of
    * stripping out the prefixNS to the API user.  So the API user will never have to deal with the prefixNS,
    * all calls to this API are made as if there is no such thing as a prefixNS.
    * Depending on the namespace context and the useNameSpace configuration, the prefixNS is added internally
    * in calls to SQS and removed in return values.
    *
    * The automatic prefixNS to queues is intended to achieve easy 'partitioning' of the SQS
    * using the same (accessKeyID, secretAccessKey) pair.  This is particularly targeted for development mode where
    * multiple developers are working possibly using the same (accessKeyID, secretAccessKey) pair.
    */
   static setupInstance(awsAccessKeyID, awsSecretAccessKey, nameSpace = null, ConfigObject config = null)
         throws SQSException {
      def key = (nameSpace) ?: AWSConnection.UNNAMED_NAMESPACE
      def sqsDriver = SQS_Drivers.get(key)
      if (!sqsDriver) {
         SQS_Drivers.putIfAbsent(key,
                                 new SQSDriver(awsAccessKeyID, awsSecretAccessKey, nameSpace, config))
         sqsDriver = SQS_Drivers.get(key)
      }
      else {
         def msg = """setupInstance method already called for SQS nameSpace:${nameSpace},
                            awsAccessKeyID:${awsAccessKeyID}, awsSecretAccessKey:${awsSecretAccessKey}
                        """.toString()
         logger.error(msg)
         throw new SQSException(msg)
      }
      return sqsDriver
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
    *   aws.sqs.useSSLConnection = true
    *   aws.sqs.useNameSpace = false
    *   aws.sqs.defaultVisibilityTimeout = 30
    *   aws.sqs.defaultMaxNumberOfMessages = 1
    *   aws.sqs.http.connectionTimeout = 60000
    *   aws.sqs.http.socketTimeout = 60000
    *   aws.sqs.http.maxConnectionsPerHost = 50
    *   aws.sqs.http.staleCheckingEnabled = true
    *   aws.sqs.http.tcpNoDelay = true
    *   aws.sqs.http.httpProtocolExpectContinue = true
    *   aws.sqs.http.numberRetries = 5
    * @return                      SQSDriver instance
    */
   static setupInstance(awsAccessKeyID, awsSecretAccessKey, nameSpace=null, Properties config=null) {
      def configObject = getConfigObject(config)
      // Convert to correct data types
      setupConfigProps(configObject)
      return setupInstance(awsAccessKeyID, awsSecretAccessKey, nameSpace, configObject)
   }

   /**
    * Converts the properties to correct data types.
    *   aws.sqs.defaultVisibilityTimeout = 30
    *   aws.sqs.defaultMaxNumberOfMessages = 1
    */
   private static setupConfigProps(configObject) {
      if (!configObject) {
         return
      }

      AWSConnection.setupConfigProps(configObject.aws?.sqs)

      try {
         String vto = configObject.aws?.sqs?.getProperty(DEFAULT_VISIBILITY_TIMEOUT_KEY)
         if (vto)    {
            configObject.aws.sqs.setProperty(DEFAULT_VISIBILITY_TIMEOUT_KEY, Integer.parseInt(vto))
         }

         String maxMsgs = configObject.aws?.sqs?.getProperty(DEFAULT_MAX_NUMBER_OF_MESSAGES_KEY)
         if (maxMsgs)    {
            configObject.aws.sqs.setProperty(DEFAULT_MAX_NUMBER_OF_MESSAGES_KEY, Integer.parseInt(maxMsgs))
         }
      }
      catch (Exception e) {
         def msg = "SQS setupConfigProps Conversion Error"
         logger.error(msg)
         throw new SQSException(msg, e)
      }
   }

   /**
    * @param nameSpace    user specified name space, default null - no name required
    * @return             SQSriver instance associated with the nameSpace; exception thrown if operation failed
    *
    * Note: This method cannot be called for a nameSpace before the setupInstance method is called.
    */
   static getInstance(nameSpace = null) throws SQSException {
      def key = (nameSpace) ?: AWSConnection.UNNAMED_NAMESPACE
      def sqsDriver = SQS_Drivers.get(key)
      if (!sqsDriver) {
         def msg = "getInstance method called for SQS nameSpace:${nameSpace} before setupInstance method".toString()
         logger.error(msg)
         throw new SQSException(msg)
      }
      return SQS_Drivers.get(key)
   }

   /**
    * Create a new queue
    *
    * @param queueName                 queue to be created in valid SQS queue name format
    * @param defaultVisibilityTimeout  the visibility timeout (in seconds) to use for this queue
    * @return                          true if domain newly created
    *                                  exception thrown if operation failed or queue already exists
    */
   def createQueue(queueName, defaultVisibilityTimeout = 0) throws SQSException {
      def queue = prefixNS + queueName

      if (queueExists(queue)) {
         def err = "createQueue failed: ${queue} already exists".toString()
         logger.error(err)
         throw new SQSException(err)
      }

      def dvt = defaultVisibilityTimeout ?: this.defaultVisibilityTimeout

      def (statusCode, createQueueResponse) = doPostRequest(sqsUrl,
                                                            [["Action", "CreateQueue"],
                                                            ["DefaultVisibilityTimeout", dvt.toString ()],
                                                            ["QueueName", queue]])

      logger.info("createQueue returned {} for queue {}", createQueueResponse, queueName)
      return parseResponse(createQueueResponse)
   }

   /**
    * Delete an existing queue
    *
    * @param queueName     queue to be deleted
    * @return              true if domain is successfully deleted
    *                      exception thrown if operation failed or if the queue does not exist
    */
   def deleteQueue(queueName) throws SQSException {
      def queue = prefixNS + queueName

      if (!queueExists(queue)) {
         def err = "deleteQueue failed: ${queue} does not exist".toString()
         logger.error(err)
         throw new SQSException(err)
      }

      def (statusCode, deleteQueueResponse) = doPostRequest(sqsUrl + queue,
                                                            [["Action", "DeleteQueue"]])

      logger.info("deleteQueue returned {} for queue {}", deleteQueueResponse, queueName)
      return parseResponse(deleteQueueResponse)
   }

   /**
    * List all queue names returning a maximum of 1000 queues
    *
    * @param queueNamePrefix  used for filtering the list results (Maximum 80 characters; alphanumeric
    *                         characters, hyphens (-), and underscores (_) are allowed). Only those queues
    *                         whose name begins with the specified string are returned.
    * @return                 list of queues if successful  [queue1, queue2, ...]
    *                         empty list if there is no matching  [ ]
    *                         exception thrown if operation failed
    */
   def listQueues(queueNamePrefix = null) {
      def qnp = queueNamePrefix ? prefixNS + queueNamePrefix : prefixNS

      def (statusCode, listQueuesResponse) = doPostRequest(sqsUrl,
                                                           [["Action", "ListQueues"],
                                                           ["QueueNamePrefix", qnp]])

      logger.info("listQueues returned {} for prefix {}", listQueuesResponse, qnp)
      parseResponse(listQueuesResponse)

      def list = []
      def begin = SQS_URL + prefixNS // The URL returned by AWS is always an http
      def beginIndex = begin.size()
      def queueName

      listQueuesResponse?.ListQueuesResult?.QueueUrl?.each {
         queueName = it.toString()
         if (queueName.startsWith(begin)) {
            list << queueName[beginIndex..- 1]
         }
      }

      logger.info("list of queues {} for prefix {}", list, qnp)
      return list
   }

   /**
    * Check whether a queue exists
    *
    * @param queueName        queue to be checked if it exists
    * @return                 true if queue exists
    *                         false otherwise
    *                         exception thrown if operation failed
    */
   def queueExists(queueName) {
      return listQueues()?.contains(queueName)
   }

   /**
    * Set an attribute of a queue. Currently, you can set only the VisibilityTimeout attribute for a queue
    *
    * @param queueName     queue whose attributes are modified
    * @param attributes    key-value pairs of attributes (Map). Currently, the only attribute set is VisibilityTimeout
    * @return              true if operation is successful
    *                      exception thrown if operation failed
    */
   def setQueueAttributes(queueName, attributes) {
      def queue = prefixNS + queueName

      def (statusCode, setQueueAttributesResponse) = doPostRequest(sqsUrl + queue,
                                                                   [["Action", "SetQueueAttributes"]] +
                                                                   buildAttributes (attributes))
      logger.info("setQueueAttributes returned {} for queue {}", setQueueAttributesResponse, queueName)
      return parseResponse(setQueueAttributesResponse)
   }

   /**
    * Get one or all attributes of a queue. Queues currently have two attributes: VisibilityTimeout and
    * ApproximateNumberOfMessages
    *
    * @param queueName        queue whose attribute values are fetched
    * @param attributeName    All | VisibilityTimeout | ApproximateNumberOfMessages; default is All
    * @return                 key-value pairs of (two) attributes (Map) if operation is successful
    *                         exception thrown if operation failed
    */
   def getQueueAttributes(queueName, attributeName = "All") {
      def queue = prefixNS + queueName

      def (statusCode, getQueueAttributesResponse) = doPostRequest(sqsUrl + queue,
                                                                   [["Action", "GetQueueAttributes"],
                                                                   ["AttributeName", attributeName]])

      logger.info("getQueueAttributes returned {} for queue {}", getQueueAttributesResponse, queueName)
      parseResponse(getQueueAttributesResponse)

      def queueAttributes = [:]
      getQueueAttributesResponse?.GetQueueAttributesResult?.Attribute?.each {
         queueAttributes.put(it.Name.text(), it.Value.text())
      }

      logger.info("queue attributes are {}", queueAttributes)
      return queueAttributes
   }

   /**
    * Deliver a message to the specified queue. The maximum allowed message size is 8 KB
    *
    * @param queueName     queue where the message is delivered
    * @param message       the message to send
    * @return              a map of [MessageId : MD5OfMessageBody] if message delivery is successfully
    *                      exception thrown if operation failed
    */
   def sendMessage(queueName, message) {
      def queue = prefixNS + queueName

      def (statusCode, sendMessageResponse) = doPostRequest(sqsUrl + queue,
                                                            [["Action", "SendMessage"],
                                                            ["MessageBody", message]])

      logger.info("sendMessage returned {} for queue {}", sendMessageResponse, queueName)
      parseResponse(sendMessageResponse)

      def returnMessage = [:]

      if (sendMessageResponse?.SendMessageResult?.MessageId &&
          sendMessageResponse?.SendMessageResult?.MD5OfMessageBody) {
         returnMessage.put(sendMessageResponse?.SendMessageResult?.MessageId.text(),
                     sendMessageResponse?.SendMessageResult?.MD5OfMessageBody.text())
      }

      return returnMessage
   }

   /**
    * Retrieve one or more messages from the specified queue
    *
    * @param queueName     queue where the messages are retrieved
    * @param attributes    a map of request parameters: [MaxNumberOfMessages: value, VisibilityTimeout: value];
    *                      default is null
    * @return              a list of maps; each map is of the format:
    *                      [MessageId: value, ReceiptHandle: value, MD5OfBody: value, Body: value]
    *                      exception thrown if operation failed
    */
   def receiveMessage(queueName, attributes = null) {
      def queue = prefixNS + queueName
      def maxNumberOfMessages = attributes?.get("MaxNumberOfMessages") ?: defaultMaxNumberOfMessages
      def visibilityTimeout = attributes?.get("VisibilityTimeout") ?: defaultVisibilityTimeout

      def (statusCode, receiveMessageResponse) = doPostRequest(sqsUrl + queue,
                                                               [["Action", "ReceiveMessage"],
                                                               ["MaxNumberOfMessages", maxNumberOfMessages.toString()],
                                                               ["VisibilityTimeout", visibilityTimeout.toString()]])

      logger.info("receiveMessage returned {} for queue {}", receiveMessageResponse, queueName)
      parseResponse(receiveMessageResponse)

      def messages = []
      receiveMessageResponse?.ReceiveMessageResult?.Message?.each {
         message ->
         def messageMap = [:]
         message?.children()?.each {
            messageMap.put(it.name().toString(), it.text())
         }
         messages << messageMap
      }

      return messages
   }

   /**
    * Delete the specified message from the specified queue
    *
    * @param queueName        queue where the message is deleted
    * @param receiptHandle    the receipt handle associated with the deleted message
    * @return                 true if operation is successful
    *                         exception thrown if operation failed
    */
   def deleteMessage(queueName, receiptHandle) {
      def queue = prefixNS + queueName

      def (statusCode, deleteMessageResponse) = doPostRequest(sqsUrl + queue,
                                                              [["Action", "DeleteMessage"],                                                                  
                                                              ["ReceiptHandle", receiptHandle]])

      logger.info("deleteMessage returned {} for queue {}", deleteMessageResponse, queueName)
      return parseResponse(deleteMessageResponse)
   }

   private def buildAttributes(attributes) {
      def attribute = []
      logger.debug("buildAttributes - attributes: {},", attributes)
      attributes.each {
         key, value ->
         attribute += [["Attribute.Name", key]]
         attribute += [["Attribute.Value", value.toString()]]
      }

      return attribute
   }

   private def parseResponse(response) throws SQSException {
      def valid = (!(response.Error?.Code?.text()))
      parseResponseMetadata(response.ResponseMetadata)
      def errMsg = "Error Code: ${response.Error?.Code?.text()},  Message: ${response.Error?.Message?.text()}"

      if (!valid) {
         def msg = errMsg?.toString()
         logger.error(msg)
         throw new SQSException(msg)
      }

      return valid
   }

   private def parseResponseMetadata(responseMetadata) {
      def requestId = responseMetadata.RequestId.toString()
      logger.info("RequestID: {}", requestId)
   }
}