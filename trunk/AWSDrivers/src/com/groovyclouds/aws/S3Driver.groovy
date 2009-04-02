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
 * GroovyClouds S3 Driver
 */

package com.groovyclouds.aws

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.groovyclouds.aws.exception.AWSException
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.methods.PutMethod
import org.apache.commons.httpclient.methods.HeadMethod
import org.apache.commons.httpclient.HttpStatus


import org.apache.commons.httpclient.methods.DeleteMethod
import org.apache.commons.httpclient.methods.StringRequestEntity
import org.apache.commons.httpclient.methods.FileRequestEntity

import groovy.xml.MarkupBuilder

import java.util.concurrent.ConcurrentHashMap
import java.text.SimpleDateFormat

import com.groovyclouds.aws.exception.S3Exception

class S3Driver extends AWSConnection {

    static final def LOCATION_EU = "EU"

    static final def PAYER_REQUESTER = "Requester"
    static final def PAYER_BUCKET_OWNER = "BucketOwner"

    static final def TYPE_PLAIN_TEXT = "text/plain"
    static final def TYPE_XML_TEXT = "text/xml"

    static final def GET_HEADER_RANGE = "Range"
    static final def GET_HEADER_IF_MODIFIED_SINCE = "If-Modified-Since"
    static final def GET_HEADER_IF_UNMODIFIED_SINCE = "If-Unmodified-Since"
    static final def GET_HEADER_IF_MATCH = "If-Match"
    static final def GET_HEADER_IF_NONE_MATCH = "If-None-Match"

    static final def DEFAULT_ENCODING =  "UTF-8"


    private static final def AMZ_HEADER = "x-amz-"
    private static final def AMZ_HEADER_META = "x-amz-meta-"

    private static final def HTTP_METHOD_SSL = "https://"
    private static final def HTTP_METHOD_NON_SSL = "http://"

    private static final def S3_HOST = "s3.amazonaws.com"

    private static ConcurrentHashMap S3_Drivers = new ConcurrentHashMap()    

    private static final Logger logger = LoggerFactory.getLogger(S3Driver.class)

    private def use_ssl


    private S3Driver(awsAccessKeyID, awsSecretAccessKey, nameSpace=null, ConfigObject config=null) {
        super(awsAccessKeyID, awsSecretAccessKey, config?.aws?.s3)
        this.useNameSpace = (config?.aws?.s3?.getProperty(USE_NAMESPACE_KEY) == true)
        this.nameSpace = (nameSpace) ?: ""
        this.prefixNS = (useNameSpace && nameSpace) ? nameSpace : ""
        use_ssl = config?.aws?.s3?.getProperty(USE_SSL_CONNECTION_KEY) == false ? false : true

    }

    /**
     * @param awsAccessKeyID        AWS Access Key ID
     * @param awsSecretAccessKey    AWS Secret Access Key
     * @param nameSpace             user specified namespace for S3, default null, no name required
     * @param config                configuration object, default null to use default configuration
     * 
     * @return                      S3Driver instance
     *
     * Notes:
     * This method needs to be called before the getInstance method for the nameSpace is called.
     * Format and default values of configuration object for S3
      aws {
         s3 {
             useSSLConnection = true
             useNameSpace = false

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
     * The key "_unnamed" is used to store this instance with no namespace and no bucket prefixes are applied.
     * Once an instance is set up for a key (also namespace), it cannot be changed.  However, there is no restriction
     * in creating as many instances with same (accessKeyID, secretAccessKey) pair.
     * A prefix is automatically prefixed to bucket names when configuration 'useNameSpace = true'.
     * The prefix is transparent to users of the API and is an internal implementation detail that takes care of
     * stripping out the prefix to the API user.  So the API user will never have to deal with the prefix,
     * all calls to this API are made as if there is no such thing as a prefix.
     * Depending on the namespace context and the useNameSpace configuration, the prefix is added internally
     * in calls to S3 and removed in return values.
     *
     * The automatic prefix to bucket name feature is intended to achieve easy 'partitioning' of the S3 buckets
     * using the same (accessKeyID, secretAccessKey) pair.  This is particularly targeted for development mode where
     * multiple developers are working possibly using the same (accessKeyID, secretAccessKey) pair.
     */
    static setupInstance(awsAccessKeyID, awsSecretAccessKey, nameSpace=null, ConfigObject config=null) {
        def key = (nameSpace) ?: AWSConnection.UNNAMED_NAMESPACE
        def s3Service = S3_Drivers.get(key)
        if (!s3Service) {
            S3_Drivers.putIfAbsent(key,
                    new S3Driver(awsAccessKeyID, awsSecretAccessKey, nameSpace, config))
            s3Service = S3_Drivers.get(key)
        }
        else    {
            def msg = """setupInstance method already called for S3 namespace:${nameSpace},
                            awsAccessKeyID:${awsAccessKeyID}, awsSecretAccessKey:${awsSecretAccessKey}
                        """.toString()
            logger.error(msg)
            throw new S3Exception(msg)
        }
        return s3Service
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
     *   aws.s3.useSSLConnection = true
     *   aws.s3.useNameSpace = false
     *   aws.s3.http.connectionTimeout = 60000
     *   aws.s3.http.socketTimeout = 60000
     *   aws.s3.http.maxConnectionsPerHost = 50
     *   aws.s3.http.staleCheckingEnabled = true
     *   aws.s3.http.tcpNoDelay = true
     *   aws.s3.http.httpProtocolExpectContinue = true
     *   aws.s3.http.numberRetries = 5
     * @return                      S3Driver instance
     */
    static setupInstance(awsAccessKeyID, awsSecretAccessKey, nameSpace=null, Properties config=null) {
        def configObject = getConfigObject(config)
        // Convert to correct data types
        setupConfigProps(configObject)
        return setupInstance(awsAccessKeyID, awsSecretAccessKey, nameSpace, configObject)
    }

    /**
     * Converts the properties to correct data types.
     * maxNumDigits = 20
     * offsetValue = 1000000
     * maxDigitsLeft = 10
     * maxDigitsRight = 4
     */
    private static setupConfigProps(configObject)    {
        if (!configObject) {
         return
        }
       
        AWSConnection.setupConfigProps(configObject?.aws?.s3)
    }

    /**
     * @param nameSpace    user specified name for the schema, default null - no name required
     *
     * @return             S3Driver instance associated with the nameSpace
     *
     * Note: This method cannot be called for a nameSpace before the setupInstance method is called.
     *
     */
    static getInstance(nameSpace=null) {
        def key = (nameSpace) ?: AWSConnection.UNNAMED_NAMESPACE
        def s3Service = S3_Drivers.get(key)
        if (!s3Service) {
            def msg = "getInstance method called for S3 namespace:${nameSpace} before setupInstance method".toString()
            logger.error(msg)
            throw new IllegalStateException(msg)
        }
        return S3_Drivers.get(key)
    }

    /**
     * Returns whether the bucketName is valid DNS name as specified in S3 docs
     * @param bucketName    name of the bucket
     *
     * @return  true
     *          false      
     */
    static def isDNSName(bucketName)   {
        def valid = true
        def size = bucketName.size()
        if (size < 3 || size > 63)  {
            return false
        }
        for (name in bucketName.split("\\."))    {
            if (!name.matches(/^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/))  {
                valid = false
                break
            }
        }
        return valid
    }

    /**
     * Create a bucket with acl specified as canned access policies and location (only EU otherwise default US).
     *
     *  Canned Access Policies from the S3 documents:
     *   private—Owner gets FULL_CONTROL.
     *   public-read—Owner gets FULL_CONTROL and the anonymous principal is granted READ access.
     *       If this policy is used on an object, it can be read from a browser with no authentication.
     *   public-read-write—Owner gets FULL_CONTROL, the anonymous principal is granted READ and WRITE access.
     *       This can be a useful policy to apply to a bucket, but is generally not recommended.
     *   authenticated-read—Owner gets FULL_CONTROL, and any principal authenticated as a registered Amazon S3 user
     *      is granted READ access.
     *
     * @param bucketName
     * @param acl        (optional canned policy)
     * @param location   (optional, S3 supported value "EU")
     *
     * @return true         if successful
     *
     * @throws S3Exception      if S3 returns status code != 200 and code < 500
     *         ServiceException if S3 returns status code >= 500
     *         AWSException     other errors     
     */
    def createBucket(String bucketName, def acl=null, location=null) {
        def body
        def s3headers = [:]
        if (acl)    {
            s3headers = ['x-amz-acl': acl]
        }
        if (location)   {
            body = """<CreateBucketConfiguration><LocationConstraint>${location}</LocationConstraint></CreateBucketConfiguration>""".toString()
        }
        def (headers, response) = doPutRequest(bucketName, "/", body, s3headers)
        logger.info("Create Bucket: bucketName:{}, acl:{}, location:{}, headers:{}, response:{}",
                        bucketName, acl, location, headers, response)
    }

    /**
     * List the keys contained in a bucket. Keys are selected for listing by bucket, prefix, delimiter.
     * Pagination is done using a marker.
     * @param bucketName    Name of the bucket
     * @param prefix        Prefix of the key
     * @param marker        Marker for pagination, default ""
     * @param max_keys      max_keys to return, default 20
     * @param delimiter     delimiter for the keys, default "/"
     *
     * @return Original Query (Map), Keys (List), Common Prefix (List), NextMarker (String)
     */
    def listKeys(String bucketName, prefix="", marker="", max_keys=20, delimiter="/")  {
        def (headers, response) = doGetRequest(bucketName,
                    "/?prefix=${prefix}&marker=${marker}&max-keys=${max_keys}&delimiter=${delimiter}")
        def query = [:]
        query.put("BucketName", response.Name?.text())
        query.put("Prefix", response.Prefix?.text())
        query.put("Marker", response.Marker?.text())
        query.put("MaxKeys", response.MaxKeys?.text())
        query.put("Delimiter", response.Delimiter?.text())
        query.put("IsTruncated", response.IsTruncated?.text())

        def keys = []
        response.Contents?.each { c ->
            def bucketContent = [:]
            bucketContent.put("Key", c.Key.text())
            bucketContent.put("LastModified", c.LastModified.text())
            bucketContent.put("ETag", c.ETag.text())
            bucketContent.put("Size", c.Size.text())
            bucketContent.put("OwnerID", c.Owner.ID.text())
            bucketContent.put("OwnerDisplayName", c.Owner.DisplayName.text())
            bucketContent.put("StorageClass", c.StorageClass.text())
            keys << bucketContent
        }
        def commonPrefix = []
        response.CommonPrefixes?.each { p ->
            if (p.Prefix)   {
                commonPrefix << p.Prefix.text()
            }
        }
        def nextMarker = response.NextMarker
        if (! nextMarker)    {
            nextMarker = keys.length() > prefix.length() ? keys.length() : prefix.length()
        }
        logger.info("listKeys: headers:{}, query:{}, keys:{}, common prefix{}, nextMarker{}",
                                headers, query, keys, commonPrefix, nextMarker)
        return [query, keys, commonPrefix, nextMarker]
    }

    /**
     * Returns the bucket location.
     *
     * @param bucketName
     *
     * @return Location
     */
    def getBucketLocation(bucketName)   {
        def (headers, response) = doGetRequest(bucketName, "/?location")
        def location = response.text() ?: "US"
        logger.info("getBucketLocation for bucket:{}, headers:{}, Location:{}",
                            bucketName, headers, location)
        return location
    }

    /**
     * Set Payer configuration for a bucket.
     *
     * @param bucketName
     * @param payer             Allowed values: BucketOwner, Requester
     *
     * @throws S3Exception, ServiceException, AWSException
     */
    def setRequestPayment(bucketName, payer)    {
        def body = """
                    <RequestPaymentConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                      <Payer>$payer</Payer>
                    </RequestPaymentConfiguration>
                    """.toString()
        def reqEntity = new StringRequestEntity(body, "text/xml", "utf-8")
        def (headers, response) = doPutRequest(bucketName, "/?requestPayment", reqEntity)
        logger.info("Set RequestPayment: bucketName:{}, payer:{}, headers:{}, response:{}",
                            bucketName, payer, headers, response)
    }

    /**
     * Get the Payer configuration for a bucket.
     *
     * @param bucketName
     * 
     * @return Payer (BucketOwner or Requester)
     *
     */
    def getRequestPayment(bucketName)   {
        def (headers, response) = doGetRequest(bucketName, "/?requestPayment")
        logger.info("Get RequestPayment for bucket:{}, headers:{}, Payer:{}",
                            bucketName, headers, response?.Payer?.text())
        return response?.Payer?.text()
    }

    /**
     * Set the ACL configuration for a bucket.
     *
     * @param bucketName
     * @param ownerID
     * @param ownerDisplayName
     * @param grants                List of S3Grant objects
     *
     * @throws S3Exception, ServiceException, AWSException
     *
     */
    def setBucketACL(bucketName, ownerID, ownerDisplayName, grants) {
        def reqEntity = new StringRequestEntity(S3Grant.toXML(ownerID, ownerDisplayName, grants), TYPE_XML_TEXT, DEFAULT_ENCODING)
        def (headers, response) = doPutRequest(bucketName, "/?acl", reqEntity)
        logger.info("setBucketACL bucketName:{}, ownerID:{}, ownerDisplayName:{}, grants:{}, headers:{}, response:{}",
                        bucketName, ownerID, ownerDisplayName, grants, headers, response)
    }

    /**
     * Returns the ACL configuration of a bucket.
     *
     * @param bucketName
     *
     * @return List of S3Grant objects
     */
    def getBucketACL(bucketName) {
        def (headers, response) = doGetRequest(bucketName, "/?acl")
        logger.info("Get Bucket ACL: bucketName:{}, headers:{} response:{}", bucketName, headers, response)
        def grants= S3Grant.fromXML(bucketName, response)
        return grants
    }

    /**
     * List the buckets.
     *
     * @return owner, List of buckets
     */
    def listBuckets()   {
        def (headers, response) = doGetRequest()
        def owner = ["ID": response.Owner?.ID.text(), "DisplayName": response.Owner?.DisplayName.text()]
        def buckets = []
        def name
        response.Buckets?.Bucket?.each { bucket ->
            name = bucket.Name.text()
            if (name.startsWith(prefixNS))   {
               buckets << ["Name": name.substring(prefixNS.length()),
                           "CreationDate":bucket.CreationDate.text()]
            }
        }
        logger.info("headers={}, owner={}, buckets={}", headers, owner, buckets)
        return [owner, buckets]
    }

    /**
     * Set bucket logging.
     * Note that the source and the target buckets must be in the same location.
     * The source bucket needs to be set up with READ_ACP and WRITE permissions for the
     * S3LoggingStatus.GROUP_LOG_DELIVERY before logging can be enabled.
     *
     * @param bucketName    Name of the bucket
     * @param logStatus     S3LoggingStatus object
     *
     */
    def setBucketLoggingStatus(bucketName, S3LoggingStatus logStatus)  {
        def logXML = S3LoggingStatus.toXML(logStatus)
        def reqEntity = new StringRequestEntity(logXML, TYPE_XML_TEXT, DEFAULT_ENCODING)
        logger.info("setBucketLoggingStatus: bucketName={}, logStatus:{}, logXML:{}", bucketName, logStatus, logXML)
        def (headers, response) = doPutRequest(bucketName, "/?logging", reqEntity)
        logger.info("setBucketLoggingStatus: headers={}, response:{}", headers, response)
    }

    /**
     * Retrieves the bucket loggins status.
     *
     * @param bucketName        Name of the bucket
     *
     * @return S3LoggingStatus object
     * 
     */
    def getBucketLoggingStatus(bucketName)  {
        def (headers, response) = doGetRequest(bucketName, "/?logging")
        def logStatus = S3LoggingStatus.fromXML(response)
        logger.info("getBucketLoggingStatus: bucketName={}, logStatus:{}",
                bucketName, logStatus)
        return logStatus
    }

    /**
     * Delete a bucket.
     *
     * @param bucketName    bucket to delete
     *
     * @throws S3Exception, ServiceException, AWSException
     * 
     */
    def deleteBucket(bucketName)    {
        def delResponse = doDeleteRequest(bucketName)
        logger.info("delete bucket:{}, response:{}", bucketName, delResponse)
    }


    /**
     * Determines whether a bucket name exists, using HEAD command by specifying the name of the bucket
     * and setting max-keys to 0.
     *
     * @param bucketName
     *
     * @return true/false
     * 
     */
    def bucketExists(bucketName)    {
        def (headers, response) = doHeadRequest(bucketName, "/?keys=0")
        def exists = headers.get("StatusCode") == HttpStatus.SC_OK
        logger.info("Bucket Name {} exists {}", bucketName, exists)
        return exists
    }

    /**
     * Store a text object with optional metadata and canned ACL for a specific key in a bucket.
     *
     * @param bucketName
     * @param key
     * @param text
     * @param metadata           Optional Map of metadata associated with the key+text. Value can be a list.
     * @param type               plain text, xml
     * @param encoding           character encoding
     * @param acl                canned ACL policy - See S3Grant
     * @param requestheaders     additional request headers - see S3 doc for more info such as
     *                           Content-MD5, Content-Disposition etc
     * 
     * @return Response Headers (Map), Response
     *
     * @throws S3Exception, ServiceException, AWSException
     * 
     */
    def addTextObject(bucketName, key, String text, Map metadata=null,
                  String type=TYPE_PLAIN_TEXT, String encoding=DEFAULT_ENCODING, acl=null, requestheaders=[:])  {
        logger.info("AddTextObject- bucketName:{}, key:{}, text:{}, metadata:{}, type:{}, encoding:{}, acl:{}, requestheaders={}",
                        bucketName, key, text, metadata, type, encoding, acl, requestheaders)
        def reqEntity = new StringRequestEntity(text, type, encoding)
        def (headers, response) = addObject(bucketName, key, reqEntity, metadata, acl, requestheaders)
        logger.info("AddTextObject: headers:{}, response:{}", headers, response)
        return [headers, response]
    }

    /**
     * Store a file content with optional metadata and canned ACL for a specific key in a bucket.
     *
     * @param bucketName
     * @param key
     * @param fileName
     * @param fileType
     * @param metadata           Optional Map of metadata associated with the key+text. Value can be a list.
     * @param acl                canned ACL policy - See S3Grant
     * @param requestheaders     additional request headers - see S3 doc for more info such as
     *                           Content-MD5, Content-Disposition etc
     *
     * @return Response Headers (Map), Response
     * 
     * @throws S3Exception, ServiceException, AWSException
     *
     */
    def addFileObject(bucketName, key, String fileName, String fileType, Map metadata=null, acl=null, requestheaders=[:])  {
        logger.info("AddFileObject: bucketName:{}, key:{}, fileName:{},  fileType:{}, metadata:{}, acl:{}",
                                bucketName, key, fileName, fileType, metadata, acl)
        def reqEntity = new FileRequestEntity(new File(fileName), fileType)
        def (headers, response) = addObject(bucketName, key, reqEntity, metadata, acl, requestheaders=[:])
        logger.info("AddFileObject: headers:{}, response:{}", headers, response)
        return [headers, response]
    }

    /**
     * Returns information about the object stored in a bucket with specified key.
     *
     * @param bucketName
     * @param objectKey
     *
     * @return Map of headers with information about the object
     * 
     */
    def getObjectInfo(bucketName, objectKey)   {
        def (headers, response) = doHeadRequest(bucketName, "/${objectKey}".toString())
        logger.info("Get ObjectInfo: bucketName:{}, objectKey: {}, responseHeaders:{}, response:{}",
                        bucketName, objectKey, headers, response)
        return headers
    }

    /**
     * Retrieve the text information stored in a bucket and specified key.
     *
     * @param bucketName
     * @param key
     *
     * @return headers (Map), text (String)
     */
    def getTextObject(bucketName, key)  {
        def (headers, response) = doGetRequest(bucketName, "/${key}".toString(), false)
        logger.info("getTextObject: bucketName:{}, key: {}, responseHeaders:{}, response:{}",
                        bucketName, key, headers, response)
        return [headers, response]    
    }

    /**
     * Retrieve the file stored in a bucket and specified key and write it in a specified directory.
     *
     * @param bucketName
     * @param key
     * @param writeFile     true or false (to write file to disk or not)
     * @param baseDir       Local directory to store the file, default is "../"
     *
     * @return headers (Map)
     */
    def getFileObject(bucketName, key, writeFile = true, baseDir = "../", requestHeaders = [:])  {
        def handlerConfig = [writeFile:writeFile, fileName: "${baseDir}${key}".toString()] 
        def (headers, response) = doGetRequest(bucketName, "/${key}".toString(), false, handlerConfig, requestHeaders)
        logger.info("getFileObject: bucketName:{}, baseDir:{}, key: {}, responseHeaders:{}, response:{}",
                        bucketName, baseDir, key, headers, response)
        return [headers, response]
    }

    /**
     * Copy an object from one bucket to another bucket with optional metadata and canned ACL.
     *
     * @param bucketName
     * @param key
     * @param copySource (S3CopySource)
     * @param metadata   (Map)                          Optional metadata
     * @param acl        (S3Grant)                      canned ACL policy - See S3Grant
     *
     * @return Response Headers (Map), lastModified (String), ETag (String)
     * 
     */
    def copyObject(bucketName, key, S3CopySource copySource, Map metadata=null, acl=null) {
        def (headers, response) = addObject(bucketName, key, null, metadata, acl, copySource.getHeaders())
        def lastModified = response?.LastModified?.text()
        def etag = response?.ETag?.text()
        logger.info("copyObject: bucketName:{}, key:{}, copySource:{}, metadata:{}, acl={}, responseHeaders:{}, lastModified:{}, etag:{}",
                bucketName, key, copySource, metadata, acl, headers, lastModified, etag)
        return [headers, lastModified, etag]
    }

    /**
     * Set the grants for an object.
     *
     * @param bucketName
     * @param key
     * @param ownerID
     * @param ownerDisplayname
     * @param grants                    List of S3Grant
     *
     * @return Response Headers (Map), response
     */
    def setObjectACL(bucketName, key, ownerID, ownerDisplayName, grants) {
        def reqEntity = new StringRequestEntity(S3Grant.toXML(ownerID, ownerDisplayName, grants), TYPE_XML_TEXT, DEFAULT_ENCODING)
        def (headers, response) = doPutRequest(bucketName, "/${key}?acl".toString(), reqEntity)
        logger.info("setObjectACL bucketName:{}, key:{}, ownerID:{}, ownerDisplayName:{}, grants:{}, headers:{}, response:{}",
            bucketName, key, ownerID, ownerDisplayName, grants, headers, response)
        return [headers, response]
    }

    /**
     * Get ACL for an object.
     *
     * @param bucketName
     * @param key
     *
     * @return S3Grant
     * 
     */
    def getObjectACL(bucketName, key) {
        def (headers, response) = doGetRequest(bucketName, "/${key}?acl".toString())
        logger.info("Get Bucket ACL: bucketName:{}, headers:{} response:{}", bucketName, headers, response)
        def s3grant = S3Grant.fromXML("bucketName/${key}", response)
        return s3grant
    }


    /**
     * Delete an object.
     *
     * @param bucketName
     * @param objectKey
     *
     * @return Response Headers, response
     *
     */
    def deleteObject(bucketName, objectKey)    {
        def delResponse = doDeleteRequest(bucketName, "/${objectKey}".toString())
        logger.info("deleteObject: bucketName{}, objectKey:{}, response:{}", bucketName, objectKey, delResponse)
        return delResponse
    }

    protected String getFormattedTimestamp() {
        SimpleDateFormat df = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss", Locale.US)
        df.setTimeZone(TimeZone.getTimeZone("GMT"))
        return df.format(new Date()) + " GMT"

    }

    private def addObject(bucketName, key, reqEntity, Map metadata=null, acl=null, s3headers=[:])   {
        metadata?.each { k, v ->
            s3headers.put("x-amz-meta-${k}".toString(), v)
        }
        if (acl)    {
            s3headers.put("x-amz-acl", acl)
        }
        doPutRequest(bucketName, "/${key}".toString(), reqEntity, s3headers, ["ETag"])
    }

    private def callS3(httpMethod, s3headers, List responseHeaders=null, parseXMLResponse=true, handlerConfig=null)   {
        s3headers.each { k, v ->
            httpMethod.setRequestHeader(k, v)
        }
        def (statusCode, response) = connectAWS(httpMethod, parseXMLResponse, handlerConfig)
        if (parseXMLResponse)   {
            parseResponse(response)
        }
        def resultHeaders = getResponseHeaders(httpMethod, responseHeaders)
        resultHeaders.put("StatusCode", statusCode)
        return [resultHeaders, response]    
    }

    private def doHeadRequest(String bucketName, String path="/") throws AWSException {
        bucketName = "${prefixNS}${bucketName}".toString()        
        Map s3headers = [:]
        setupS3Headers("HEAD", s3headers, bucketName, path)
        def url = getS3url(bucketName, path)
        logger.info("HEAD request-url ={}, headers={}, bucketName={}, path={}", url, s3headers, bucketName, path)
        def httpMethod = new HeadMethod(url)
        httpMethod.setFollowRedirects(true)
        return callS3(httpMethod, s3headers)
    }

    private def doGetRequest(String bucketName=null, String path="/", parseXMLResponse=true, handlerConfig=null,
                                s3headers=[:], responseheaders=null) throws AWSException {
        bucketName = (bucketName) ? "${prefixNS}${bucketName}".toString() : bucketName
        setupS3Headers("GET", s3headers, bucketName, path)
        def url = getS3url(bucketName, path)
        logger.info("GET request - url = {}, headers = {}, bucketName={}, path={}", url, s3headers, bucketName, path)
        def httpMethod = new GetMethod(url)
        httpMethod.setFollowRedirects(true)
        return callS3(httpMethod, s3headers, responseheaders, parseXMLResponse, handlerConfig)
    }

    private def doPutRequest(bucketName, path="/", body, Map s3headers=[:], responseheaders=["Location"]) throws AWSException {
        bucketName = "${prefixNS}${bucketName}".toString()        
        def contentType = body?.getContentType()
        if (contentType)    {
            s3headers.put("Content-Type", contentType)
        }
        setupS3Headers("PUT", s3headers, bucketName, path)
        def contentLength = (body) ? body.getContentLength().toString() : "0"
        s3headers.put("Content-Length", contentLength)
        def url = getS3url(bucketName, path)        
        logger.info("PUT request - url = {}, headers = {}, body = {}", url, s3headers, body)
        def httpMethod = new PutMethod(url)
        if (body)   {
            httpMethod.setRequestEntity(body)
        }
        return callS3(httpMethod, s3headers, responseheaders)
    }

    private def doDeleteRequest(bucketName, path="/") throws AWSException {
        bucketName = "${prefixNS}${bucketName}".toString()        
        def s3headers=[:]
        setupS3Headers("DELETE", s3headers, bucketName, path)
        def url = getS3url(bucketName, path)        
        logger.info("DELETE request - url={}, headers={}, bucketName={}", url, s3headers, bucketName)
        def httpMethod = new DeleteMethod(url)
        return callS3(httpMethod, s3headers)
    }

    private def getS3url(bucketName, path)  {
        def host = "${S3_HOST}"
        if (bucketName) {
            host = isDNSName(bucketName) ?  "${bucketName}.${S3_HOST}" : "${S3_HOST}/${bucketName}"
        }
        def url = ((use_ssl) ? "${HTTP_METHOD_SSL}$host$path" : "${HTTP_METHOD_NON_SSL}$host$path").toString()
        return url
    }

    private def setupS3Headers(action, s3headers, bucketName, path) {
        s3headers.put("Date", getFormattedTimestamp())
        def stringToSign = getStringToSign(action, s3headers, bucketName, path)
        s3headers.put("Authorization", "AWS ${awsAccessKeyID}:${generateSignature(stringToSign)}".toString())
    }

    private def getStringToSign(method, headers, bucketName, path) {
        logger.info("getSignatureString method={}, headers={}, bucketName={}, path={}", method, headers, bucketName, path)
        StringBuilder strbuf = new StringBuilder()
        strbuf.append("${method}\n")
        def contentMD5 = headers.get("Content-MD5") ?: ""
        strbuf.append("${contentMD5}\n")
        def contentType = headers.get("Content-Type") ?: ""
        strbuf.append("${contentType}\n")
        strbuf.append("${headers.get("Date")}\n")
        strbuf.append(canonicalizedAmzHeaders(headers))
        strbuf.append(canonicalizedResource(bucketName, path))
        def signatureString = strbuf.toString()
        logger.info("signatureString  ={}", signatureString)
        return signatureString
    }

    private def canonicalizedAmzHeaders(headers) {
        if (!headers)   {
            return ""
        }
        def amzHeaders = new TreeMap()
        headers.each { key, value->
            if (key.startsWith(AMZ_HEADER)) {
                amzHeaders.put(key.toLowerCase(), value)
            }
        }
        StringBuilder strbuf = new StringBuilder()
        amzHeaders.each { key, value->
            strbuf.append("${key}:${value}\n")
        }
        return strbuf.toString()
    }

    private def canonicalizedResource(bucketName, path) {
        StringBuilder strbuf = new StringBuilder()
        if (bucketName) {
            strbuf.append("/")
            strbuf.append(bucketName)
        }
        def pathStr = path ? path.replaceAll(/\?.*$/, '') : ""
        strbuf.append(pathStr)
        if (path?.matches(/.*[&\?]acl(=|&|$).*/))   {
            strbuf.append('?acl')
        }
        if (path?.matches(/.*[&\?]requestPayment(=|&|$).*/))   {
            strbuf.append('?requestPayment')
        }
        if (path?.matches(/.*[&\?]torrent(=|&|$).*/))   {
            strbuf.append('?torrent')
        }
        if (path?.matches(/.*[&\?]location(=|&|$).*/))   {
            strbuf.append('?location')
        }
        if (path?.matches(/.*[&\?]logging(=|&|$).*/))   {
            strbuf.append('?logging')
        }
        return strbuf.toString()
    }

    private def getResponseHeaders(httpMethod, keys=null)    {
        def headers = [:]
        def metaHeaderLen = AMZ_HEADER_META.length()
        def hdrName
        def hdrValue

        def responseHeaders = httpMethod.getResponseHeaders()
        def metadata = [:]
        headers.put("MetaData", metadata)
        responseHeaders.each { hdr ->
            hdrName = hdr.getName().toLowerCase()
            hdrValue = hdr.getValue()
            if (hdrName.startsWith(AMZ_HEADER_META)) {
                metadata.put(hdr.getName().substring(metaHeaderLen), hdrValue)
            }
            else if (hdrName.startsWith(AMZ_HEADER))  {
                headers.put(hdr.getName(), hdrValue)
            }
        }
        headers.put("Server", httpMethod.getResponseHeader("Server")?.getValue())
        headers.put("Content-Length", httpMethod.getResponseHeader("Content-Length")?.getValue())
        headers.put("Date", httpMethod.getResponseHeader("Date")?.getValue())

        keys?.each {
            headers.put(it, httpMethod.getResponseHeader(it)?.getValue())
        }
        return headers
    }

    private def parseResponse(response)  throws S3Exception {
        def valid = ( ! (response && response?.name() == "Error"))

        if ( ! valid) {
            def errMsg = """Code:${response.Code.text()},
                            Message:${response.Message.text()},
                            Resource:${response.Resource.text()},
                            RequestId:${response.RequestId.text()}
                          """.toString()    
            logger.error("S3Driver Error:{}", errMsg)
            throw new S3Exception(errMsg)
        }
    }


}
/**
 * S3Grant encapsulates the grants for S3 bucket and objects.
 * This class is used in set and get of grants.
 * See S3 documentation for more details.
 */
class S3Grant{
    public static final def GRANTEE_OWNER = "Owner"
    public static final def GRANTEE_CUSTOMER_EMAIL = "AmazonCustomerByEmail"
    public static final def GRANTEE_CANONICAL_USER = "CanonicalUser"
    public static final def GRANTEE_GROUP = "Group"
    public static final def GROUP_AUTHENTICATED_USERS = "GROUP_AUTHENTICATED_USERS"
    public static final def GROUP_ALL_USERS = "GROUP_ALL_USERS"
    public static final def GROUP_LOG_DELIVERY = "GROUP_LOG_DELIVERY"

    public static final def GROUP_AUTHENTICATED_USERS_URI = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
    public static final def GROUP_LOG_DELIVERY_URI = "http://acs.amazonaws.com/groups/s3/LogDelivery"
    public static final def GROUP_ALL_USERS_URI = "http://acs.amazonaws.com/groups/global/AllUsers"

    public static final def PERMISSION_READ = "READ"
    public static final def PERMISSION_READ_ACP = "READ_ACP"
    public static final def PERMISSION_WRITE = "WRITE"
    public static final def PERMISSION_WRITE_ACP = "WRITE_ACP"
    public static final def PERMISSION_FULL_CONTROL = "FULL_CONTROL"

    public static final def ACL_PRIVATE = "private"
    public static final def ACL_PUBLIC_READ = "public-read"
    public static final def ACL_PUBLIC_READ_WRITE = "public-read-write"
    public static final def ACL_AUTHENTICATED_READ = "authenticated-read"

    public static final def ID = "ID"
    public static final def DISPLAY_NAME = "DisplayName"
    public static final def EMAIL_ADDRESS = "EmailAddress"
    
    private static final def ACCESS_CONTROL_POLICY = "AccessControlPolicy"
    private static final def ACCESS_CONTROL_LIST = "AccessControlList"
    private static final def PERMISSION = "Permission"
    private static final def URI = "URI"
    private static final def GRANTEE = "Grantee"
    private static final def XML_NS = "http://www.w3.org/2001/XMLSchema-instance"

    def ownerID
    def ownerDisplayName
    def grants = []             // List of Maps, each Map is an ACL key-value pair

    String toString()  {
        return "ownerID:${ownerID}, ownerDisplayName:${ownerDisplayName}, grants:${grants}".toString()
    }

    static def fromXML(resource, response)   {
        def s3grant = new S3Grant()

        s3grant.ownerID =   response.Owner?.ID
        s3grant.ownerDisplayName = response.Owner?.DisplayName

        S3Grant.getGrantList(response.AccessControlList, s3grant.grants)
        S3Driver.logger.info("Get ACL: resource: {}, s3grant:{}", resource, s3grant)
        return s3grant
    }

    static def getGrantList(grantXML, s3grantList)   {
        grantXML?.Grant?.each { grant ->
            def acl = [:]
            acl.type = grant.Grantee.@type.text()
            acl.ID = grant.Grantee.ID.text()
            acl.DisplayName = grant.Grantee.DisplayName.text()
            acl.EmailAddress = grant.Grantee.EmailAddress.text()
            acl.URI = grant.Grantee.URI.text()
            acl.Permission = grant.Permission.text()
            s3grantList << acl
        }        
    }

    /**
     * Generates XML representation of a S3 Grant.
     *
     * @param ownerID
     * @param ownerDisplayName
     * @param grants                List of S3Grant
     *
     * @return String               XML string
     * 
     */
    static def toXML(ownerID, ownerDisplayName, grants)  {
        def writer = new StringWriter()
        def xml = new MarkupBuilder(writer)
        xml."${ACCESS_CONTROL_POLICY}"()  {
            Owner   {
                ID("${ownerID}")
                DisplayName("${ownerDisplayName}")
            }

            S3Grant.addGrantListXML(xml, "${ACCESS_CONTROL_LIST}", grants)
            /*"${ACCESS_CONTROL_LIST}"    {
                grants.each {grant ->
                    Grant {
                        Grantee("xmlns:xsi":"${XML_NS}", "xsi:type":"${grant.type}") {
                            switch(grant.type)  {
                                case 'AmazonCustomerByEmail':
                                    EmailAddress(grant.EmailAddress)
                                break

                                case 'CanonicalUser':
                                case 'Owner':
                                    ID(grant.ID)
                                    DisplayName(grant.DisplayName)
                                break

                                case 'Group':
                                if (grant.groupType == S3Grant.GROUP_AUTHENTICATED_USERS)  {
                                    URI(S3Grant.GROUP_AUTHENTICATED_USERS_URI)
                                }
                                else if (grant.groupType == S3Grant.GROUP_ALL_USERS)  {
                                   URI(S3Grant.GROUP_ALL_USERS_URI)
                                }
                                else if (grant.groupType == S3Grant.GROUP_LOG_DELIVERY)  {
                                   URI(S3Grant.GROUP_LOG_DELIVERY_URI)
                                }
                                break              
                            }
                        }
                        Permission(grant.Permission)
                    }
                }
            }*/

        }
        return writer.toString()
    }

    /**
     * Generates the XML for a list of grants in the given MarkupBuilder object.
     *
     * @param xml       MarkupBuilder
     * @param tag       tag string
     * @param grants    List of S3Grant
     *
     */
    static def addGrantListXML(xml, tag, grants) {
        if (!grants || grants.size() == 0)  {
            return
        }
        xml."${tag}"()  {
            grants.each {grant ->
                Grant {
                    Grantee("xmlns:xsi":"${XML_NS}", "xsi:type":"${grant.type}") {
                        switch(grant.type)  {
                            case 'AmazonCustomerByEmail':
                                EmailAddress(grant.EmailAddress)
                            break

                            case 'CanonicalUser':
                            case 'Owner':
                                ID(grant.ID)
                                DisplayName(grant.DisplayName)
                            break

                            case 'Group':
                            if (grant.groupType == S3Grant.GROUP_AUTHENTICATED_USERS)  {
                                URI(S3Grant.GROUP_AUTHENTICATED_USERS_URI)
                            }
                            else if (grant.groupType == S3Grant.GROUP_ALL_USERS)  {
                               URI(S3Grant.GROUP_ALL_USERS_URI)
                            }
                            else if (grant.groupType == S3Grant.GROUP_LOG_DELIVERY)  {
                               URI(S3Grant.GROUP_LOG_DELIVERY_URI)
                            }
                            break
                        }
                    }
                    Permission(grant.Permission)
                }
            }
        }
    }
}

/**
 * Helper class to handle logging configuration to and from XML.
 * Set the properties of the object to get the XML version.
 */
class S3LoggingStatus {

    def loggingEnabled = true
    def targetBucket
    def targetPrefix            
    def targetGrants = []       //list of S3Grant objects

    static def toXML(S3LoggingStatus logStatus) {    
        def writer = new StringWriter()
        def xml = new MarkupBuilder(writer)
        xml.BucketLogggingStatus(xmlns:"http://s3.amazonaws.com/doc/2006-03-01/")   {
            if (logStatus.loggingEnabled) {
                LoggingEnabled {
                    TargetBucket(logStatus.targetBucket)
                    if (logStatus.targetPrefix)   {
                        TargetPrefix(logStatus.targetPrefix)
                    }
                    S3Grant.addGrantListXML(xml, "TargetGrants", logStatus.targetGrants)
                }
            }
        }
        return writer.toString()
    }

    static def fromXML(response)    {
        def logStatus = new S3LoggingStatus()

        logStatus.loggingEnabled = (response?.LoggingEnabled != "") ? true : false
        logStatus.targetBucket = response?.LoggingEnabled?.targetBucket?.text()
        logStatus.targetPrefix = response?.LoggingEnabled?.targetPrefix?.text()
        S3Grant.getGrantList(response?.LoggingEnabled?.TargetGrants, logStatus.targetGrants)
        return logStatus
    }

    public String toString()    {
        if (!loggingEnabled)    {
            return "loggingEnabled=false"
        }
        return "targetBucket=${targetBucket}, targetPrefix=${targetPrefix}, targetGrants=${targetGrants}".toString()
    }
}

class S3CopySource  {
    public static final def COPY_DIRECTIVE = "COPY"
    public static final def REPLACE_DIRECTIVE = "REPLACE"
    public static final def COPY_IF_ETAG_MATCH = "x-amz-copy-source-if-match"
    public static final def COPY_IF_ETAG_NONE_MATCH = "x-amz-copy-source-if-none-match"
    public static final def COPY_IF_UNMODIFIED_SINCE = "x-amz-copy-source-if-unmodified-since"    
    public static final def COPY_IF_MODIFIED_SINCE = "x-amz-copy-source-if-modified-since"

    public static final def META_DATA_DIRECTIVE = "x-amz-metadata-directive"
    public static final def COPY_SOURCE = "x-amz-copy-source"

    def headers = [:]

    /**
     * Helper class to generate headers for copying an object from a source to another bucket + key.
     * Construct that holds copy source related data.
     * 
     * @param source                    bucket name of source
     * @param metadataDirective         directive : COPY | REPLACE, default is COPY
     * @param otherHeaders              other headers such as ETag and Time specifications
     * 
     */   
    S3CopySource(source, metadataDirective=COPY_DIRECTIVE, otherHeaders=null) {
        headers.put(COPY_SOURCE, source)        
        headers.put(META_DATA_DIRECTIVE, metadataDirective)

        otherHeaders?.each { k, v ->
            headers.put(k, v)
        }
    }

    public String toString()    {
        return "headers:${headers}".toString()
    }
}