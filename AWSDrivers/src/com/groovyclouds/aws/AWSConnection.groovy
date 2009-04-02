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
 * GroovyClouds AWSConnection
 */
package com.groovyclouds.aws

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import sun.misc.BASE64Encoder

import org.apache.commons.httpclient.*
import org.apache.commons.httpclient.methods.*
import org.apache.commons.httpclient.params.*

import org.xml.sax.SAXException

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.text.SimpleDateFormat

import com.groovyclouds.aws.exception.*

/**
 * Base class that encapsulates connecting to AWS using the AWS REST API using the access keyID 
 * and the secret access key.
 */
protected class AWSConnection {
    public static final def USE_SSL_CONNECTION_KEY = "useSSLConnection"
    public static final def USE_NAMESPACE_KEY = "useNameSpace"    

    public static final def CONNECTION_TIMEOUT_KEY = "connectionTimeout"
    public static final def SO_TIMEOUT_KEY = "socketTimeout"
    public static final def MAX_CONNECTIONS_PER_HOST_KEY = "maxConnectionsPerHost"
    public static final def STALE_CHECKING_ENABLED_KEY = "staleCheckingEnabled"
    public static final def TCP_NO_DELAY_KEY = "tcpNoDelay"


    public static final def HTTP_PROTOCOL_EXPECT_CONTINUE_KEY= "httpProtocolExpectContinue"
    public static final def RETRY_HANDLER_TIMES_KEY = "numberRetries"

    protected static final def UNNAMED_NAMESPACE = "_unnamed"

    private static final def SIGNATURE_VERSION_1 = "1"
    private static final def SIGNATURE_VERSION_2 = "2"

    private static final def SDS_VERSION_2007 = "2007-11-07"
    private static final def DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ"

    private static final def DEFAULT_ENCODING = "UTF-8"

    private static final def CONNECTION_TIMEOUT = 60000
    private static final def SO_TIMEOUT = 60000
    private static final def MAX_CONNECTIONS_PER_HOST = 50
    private static final def STALE_CHECKING_ENABLED = true
    private static final def TCP_NO_DELAY = true
    private static final def HTTP_PROTOCOL_EXPECT_CONTINUE = true
    private static final def RETRY_HANDLER_TIMES = 5

    final private static Logger logger = LoggerFactory.getLogger(AWSConnection.class)

    protected def awsAccessKeyID
    protected def awsSecretAccessKey
    protected HttpClient httpClient

    protected def useNameSpace    // configuration to apply namespace, default false
    protected def nameSpace       // NameSpace associated with the accessKeyID and secretAccessKey
    protected def prefixNS        // prefix that is applied to achieve namespace

    protected def sdsVersion      // prefix that is applied to achieve namespace

    static getConfigObject(Properties props) {
        if (!props) {
            return null
        }
        return new ConfigSlurper().parse(props)
    }

    protected AWSConnection(awsAccessKeyID, awsSecretAccessKey, config=null) {
        this.awsAccessKeyID = awsAccessKeyID
        this.awsSecretAccessKey = awsSecretAccessKey
        this.sdsVersion = SDS_VERSION_2007
        setHttpClient(config)
    }

    static setupConfigProps(ConfigObject configObject)  {
        if (!configObject) {
            return
        }
        try {
            String useSSL = configObject.getProperty(USE_SSL_CONNECTION_KEY)
            if (useSSL)    {
                configObject.setProperty(USE_SSL_CONNECTION_KEY, Boolean.valueOf(useSSL))
            }
            String useNS = configObject.getProperty(USE_NAMESPACE_KEY)
            if (useNS)    {
                configObject.setProperty(USE_NAMESPACE_KEY, Boolean.valueOf(useNS))
            }
            String connTO = configObject.http?.getProperty(CONNECTION_TIMEOUT_KEY)
            if (connTO)    {
                configObject.http.setProperty(CONNECTION_TIMEOUT_KEY, Integer.parseInt(connTO))
            }
            String sockTO = configObject.http?.getProperty(SO_TIMEOUT_KEY)
            if (sockTO)    {
                configObject.http.setProperty(SO_TIMEOUT_KEY, Integer.parseInt(sockTO))
            }
            String maxConns = configObject.http?.getProperty(MAX_CONNECTIONS_PER_HOST_KEY)
            if (maxConns)    {
                configObject.http.setProperty(MAX_CONNECTIONS_PER_HOST_KEY, Integer.parseInt(maxConns))
            }
            String staleCheckingEnabled = configObject.http?.STALE_CHECKING_ENABLED_KEY
            if (staleCheckingEnabled)    {
                configObject.http.setProperty(STALE_CHECKING_ENABLED_KEY, Boolean.valueOf(staleCheckingEnabled))
            }
            String tcpNoDelay = configObject.http?.getProperty(TCP_NO_DELAY_KEY)
            if (tcpNoDelay)    {
                configObject.http.setProperty(TCP_NO_DELAY_KEY, Boolean.valueOf(tcpNoDelay))
            }
            String httpProtocolExpectContinue = configObject.http?.getProperty(HTTP_PROTOCOL_EXPECT_CONTINUE_KEY)
            if (httpProtocolExpectContinue)    {
                configObject.http.setProperty(HTTP_PROTOCOL_EXPECT_CONTINUE_KEY, Boolean.valueOf(httpProtocolExpectContinue))
            }
            String numberRetries = configObject.http?.getProperty(RETRY_HANDLER_TIMES_KEY)
            if (numberRetries)    {
                configObject.http.setProperty(RETRY_HANDLER_TIMES_KEY, Integer.parseInt(numberRetries))
            }
        }
        catch (Exception e) {
            def msg = "AWSConnection setupConfigProps Conversion Error"
            logger.error(msg)
            throw new AWSConnection(msg, e)
        }
    }

    private setHttpClient(ConfigObject config) {
        def httpConfig = config?.http
        HttpConnectionManagerParams connectionParams = new HttpConnectionManagerParams()
        connectionParams.setConnectionTimeout((httpConfig?."$CONNECTION_TIMEOUT_KEY") ?: CONNECTION_TIMEOUT)
        connectionParams.setSoTimeout((httpConfig?."$SO_TIMEOUT_KEY") ?: SO_TIMEOUT)
        connectionParams.setMaxConnectionsPerHost(HostConfiguration.ANY_HOST_CONFIGURATION,
                                                    (httpConfig?."$MAX_CONNECTIONS_PER_HOST_KEY") ?: MAX_CONNECTIONS_PER_HOST)
        connectionParams.setStaleCheckingEnabled((httpConfig?."$STALE_CHECKING_ENABLED_KEY" == false) ? false : STALE_CHECKING_ENABLED)
        connectionParams.setTcpNoDelay((httpConfig?."$TCP_NO_DELAY_KEY" == false) ? false : TCP_NO_DELAY)
        
        MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager()
        connectionManager.setParams(connectionParams)

        HttpClientParams clientParams = new HttpClientParams()
        clientParams.setBooleanParameter("http.protocol.expect-continue",
                    (httpConfig?."$HTTP_PROTOCOL_EXPECT_CONTINUE_KEY" == false) ? false : HTTP_PROTOCOL_EXPECT_CONTINUE)
        clientParams.setParameter(HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler(  (httpConfig?."$RETRY_HANDLER_TIMES_KEY") ?: RETRY_HANDLER_TIMES, true))
        this.httpClient = new HttpClient(clientParams, connectionManager)
    }

    /**
     * Get the useNameSpace boolean configuration.
     * 
     * @return      true or false
     *
     * Configuration value setup during initialization of whether to apply namespace or not.
     */
    def getUseNameSpace()    {
        return useNameSpace
    }

    /**
     * @return      nameSpace of the instance. Can be null for unnamed instance
     */
    def getNameSpace()    {
        return nameSpace
    }

    /**
     * @return      prefix for specified nameSpace for this instance
     *
     * If nameSpace is empty/null, returns ""
     */
    def getPrefixNS()    {
        return prefixNS
    }

    /**
     * GET request using Version 1 to AWS.
     *
     * @param awsUrl            server url to connect
     * @param request           List of request parameters in the format [parameter key, parameter value]
     *
     * @return  HttpStatus Code, Request XML GPathResult
     *
     */
    def doGetRequest(awsUrl, request) throws AWSException {
        def urlRequest = buildGetRequest(awsUrl, request)
        logger.info("AWS url = {}, request = {}, GET request URL = {}", awsUrl, request, urlRequest)
        def httpMethod = new GetMethod(urlRequest)
        httpMethod.setFollowRedirects(true)
        httpMethod.setRequestHeader("content-type", "application/x-www-form-urlencoded; charset=utf-8")
        return connectAWS(httpMethod)
    }

    /**
     * POST request using Version 2 to AWS.
     *
     * @param awsUrl            server url to connect
     * @param request           List of request parameters in the format [parameter key, parameter value]
     *
     * @return  HttpStatus Code, Request XML GPathResult
     *
     */
    def doPostRequest(awsUrl, request) throws AWSException {
        def httpMethod = new PostMethod(awsUrl)
        httpMethod.setRequestHeader("content-type", "application/x-www-form-urlencoded; charset=utf-8")        
        def sortedParameters = getAWSParametersVersion2()
        addParameters(sortedParameters, request)
        def hmac = getSignatureString2("POST", awsUrl, sortedParameters)
        sortedParameters.each   { k, v ->
            httpMethod.addParameter(k, v)
        }
        httpMethod.addParameter("Signature", hmac)
        logger.info("AWS url = {}, request = {}, POST request Parameters = {}", awsUrl, request, httpMethod.getParameters())
        def response = connectAWS(httpMethod)
        def redirectResponse = followRedirect(httpMethod)
        response = (redirectResponse) ?: response
        return response   
    }

    protected String getFormattedTimestamp() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        df.setTimeZone(TimeZone.getTimeZone("UTC"))
        return df.format(new Date())
    }

    protected def followRedirect(httpMethod)  {
        def response
        def followRedirect = 0
        while (httpMethod.getStatusCode() == HttpStatus.SC_TEMPORARY_REDIRECT
                        && followRedirect < 3)    {
            def location = httpMethod.getResponseHeader("Location")?.getValue()
            if (!location)  {
                break
            }
            httpMethod.setURI(new URI(location))
            response = connectAWS(httpMethod)
            followRedirect++
        }
        return response
    }

    protected def connectAWS(httpMethod, parseXMLResponse=true, handlerConfig=null) {
        def response
        def status
        try {
            status = httpClient.executeMethod(httpMethod)
            logger.info("ResponseCode: {}, Method: {}, URI: {}", status, httpMethod.getName(), httpMethod.getURI())
            if (status != HttpStatus.SC_NO_CONTENT) {
                if (handlerConfig?.get("writeFile"))    {
                    response = getResponseFile(httpMethod.getResponseBodyAsStream(), handlerConfig.get("fileName"))
                }
                else    {
                    response = getResponseString(httpMethod.getResponseBodyAsStream())
                }
            }
            logger.info("Response: {}, Method: {}, URI: {}", response, httpMethod.getName(), httpMethod.getURI())

            if (status >= 500)  {
                def err = "AWS Service Error: ${httpMethod.getName()}, URI: ${httpMethod.getURI()}, ResponseCode: ${status}".toString()
                logger.error(err)
                new ServiceException(err)
            }
        }
        catch (IOException e) {
            def err ="IOException in Request for Method: ${httpMethod.getName()}, URI: ${httpMethod.getURI()}, ResponseCode: ${status}".toString()
            logger.error(err, e)
            throw new AWSException(err, e)
        }
        catch (SAXException e) {
            def err = "SAXException in Request for Method: ${httpMethod.getName()}, URI: ${httpMethod.getURI()}, ResponseCode: ${status}".toString()
            logger.error(err, e)
            throw new AWSException(err, e)
        }
        catch (Exception e) {
            def err = "Exception in Request for Method: ${httpMethod.getName()}, URI: ${httpMethod.getURI()}, ResponseCode: ${status}".toString()            
            logger.error(err, e)
            throw new AWSException(err, e)
        }
        finally {
            httpMethod?.releaseConnection()
        }

        return [status, (response && parseXMLResponse) ? new XmlSlurper().parseText(response) : response]
    }

    /**
     * Read stream into string
     * @param input stream to read
     *
     * @return String
     */
    protected def getResponseString(inStream) throws AWSException {
        if (!inStream)  {
            return ""
        }
        def responseString
        try {
            Reader reader = new InputStreamReader(inStream, DEFAULT_ENCODING)
            StringBuilder strbuf = new StringBuilder()
            char[] c = new char[1024]
            int len
            while (0 < (len = reader.read(c))) {
                strbuf.append(c, 0, len)
            }
            responseString = strbuf.toString()
        }
        catch (Exception e) {
            def err = "getResponseString error"
            logger.error(err, e)
            throw new AWSException(err, e)
        }
        finally {
            inStream?.close()

            try {
                inStream?.close()
            }
            catch (Exception ise){
                def err = "Close inStream error"
                logger.error(err, ise)
            }
        }
        return responseString
    }

    protected def getResponseFile(inStream, fileName) throws AWSException {
        BufferedInputStream bufStream
        FileOutputStream outStream
        try {
            bufStream = new BufferedInputStream(inStream)
            File responseFile = new File(fileName)
            responseFile.getParentFile()?.mkdirs()
            outStream = new FileOutputStream(responseFile)
            byte[] c = new byte[1024]
            int len
            while (0 < (len = bufStream.read(c, 0, 1024))) {
                outStream.write(c, 0, len)
            }
        }
        catch (Exception e) {
            def err = "Write Response File error: fileName:${fileName}".toString()
            logger.error(err, e)
            throw new AWSException(err, e)
        }
        finally {
            try {
                bufStream?.close()
            }
            catch (Exception ise){
                def err = "Close inStream error: fileName:${fileName}".toString()
                logger.error(err, ise)
            }
            try {
                outStream?.close()
            }
            catch (Exception ose)   {
               def err = "Close outStream error: fileName:${fileName}".toString()
               logger.error(err, ose)
            }
        }
        return "true"
    }

    protected String urlEncodeURI(String value) throws AWSException {
        String encoded = null
        try {
            encoded = URLEncoder.encode(value, DEFAULT_ENCODING)
                                        .replace("+", "%20")
                                        .replace("*", "%2A")
                                        .replace("%7E","~")
                                        .replace("%2F", "/")
        }
        catch (UnsupportedEncodingException ex) {
            def err = "UnsupportedEncodingException for ${value}".toString()
            logger.error(err, ex)
            throw new AWSException(err, ex)
        }
        return encoded
    }


    protected String urlEncodeData(String value) throws AWSException {
        String encoded = null
        try {
            encoded = URLEncoder.encode(value, DEFAULT_ENCODING)
                                        .replace("+", "%20")
                                        .replace("*", "%2A")
                                        .replace("%7E","~")
        }
        catch (UnsupportedEncodingException ex) {
            def err = "UnsupportedEncodingException for ${value}".toString()
            logger.error(err, ex)
            throw new AWSException(err, ex)
        }
        return encoded
    }

    /**
     * Computes RFC 2104-compliant HMAC signature.
     *
     */
    protected def generateSignature(data, algorithm = "HmacSHA1") throws AWSException {
        def signature
        def signingKey = new SecretKeySpec(awsSecretAccessKey.getBytes(DEFAULT_ENCODING), algorithm)

        try {
            Mac mac = Mac.getInstance(algorithm)
            mac.init(signingKey)
            //signature = Base64.encodeBase64(mac.doFinal(data.getBytes(DEFAULT_ENCODING)))
            signature = new BASE64Encoder().encode(mac.doFinal(data.getBytes(DEFAULT_ENCODING)))
            logger.debug("Signature to Sign {}, HMAC String: {}", data, signature)
        } catch (Exception e) {
            def err = "Exception generating ${algorithm} signature for ${data}".toString()
            logger.error(err, ex)
            throw new AWSException(err, e)
        }
        return signature.toString()
    }

    private def getAWSParametersVersion1()  {
        def awsParametersMap = ["AWSAccessKeyId" : awsAccessKeyID,
                                "SignatureMethod": "HmacSHA1",
                                "SignatureVersion" : SIGNATURE_VERSION_1,
                                "Timestamp" : getFormattedTimestamp(),
                                "Version": sdsVersion]
        def sortedParameters= new TreeMap(String.CASE_INSENSITIVE_ORDER)
        sortedParameters.putAll(awsParametersMap)
        return sortedParameters
    }

    private def getAWSParametersVersion2()  {
        
        return new TreeMap(["AWSAccessKeyId" : awsAccessKeyID,
                                "SignatureMethod": "HmacSHA256",
                                "SignatureVersion" : SIGNATURE_VERSION_2,
                                "Timestamp" : getFormattedTimestamp(),
                                "Version": sdsVersion])

    }

    private def addParameters(sortedParameters, requestParameters)  {
        requestParameters?.each    {
            if (it[1])  {
                sortedParameters.put(it[0].toString(), it[1])                   
            }
        }
    }

    private def buildGetRequest(awsUrl, request) {
        def signatureData = new StringBuilder()
        def urlReq = new StringBuilder("${awsUrl}?")
        def sortedParameters = getAWSParametersVersion1(request)
        addParameters(sortedParameters, request)
        def k, v
        sortedParameters.entrySet().each { entry ->
            k = entry.getKey()
            v = entry.getValue()
            urlReq <<  k + "=" + URLEncoder.encode(v) + "&"
            signatureData << k + v
        }
        logger.debug("GET method: String to Sign for url {}, parameters {} is {}", awsUrl, sortedParameters, signatureData)
        def hmac = generateSignature(signatureData.toString())
        urlReq << "Signature="
        urlReq << URLEncoder.encode(hmac)
        return urlReq.toString()
    }

    private String getSignatureString2(method, awsUrl, sortedParameters) {
        StringBuilder signatureData = new StringBuilder()
        signatureData << "${method}"
        signatureData << "\n"
        URI endpoint
        try {
            endpoint = new URI(awsUrl.toLowerCase())
        } catch (URISyntaxException ex) {
            def err = "URISyntaxException in Request for Method: ${method}, URL: ${awsUrl}".toString()
            logger.error(err, ex)
            throw new AWSException(err, ex)
        }
        signatureData << endpoint.getHost()
        signatureData << "\n"
        String uri = endpoint.getPath()
        if ( ! uri ) {
            uri = "/"
        }
        signatureData << urlEncodeURI(uri)
        signatureData << "\n"
        def lastKey = sortedParameters.lastKey()
        sortedParameters.entrySet().each { entry ->
            signatureData <<  urlEncodeURI(entry.getKey())
            signatureData << "="
            signatureData << urlEncodeData(entry.getValue())
            if (entry.getKey() != lastKey) {
                signatureData << "&"
            }
        }
        logger.debug("POST method: String to Sign for url {}, parameters {} is {}", awsUrl, sortedParameters, signatureData)
        def hmac = generateSignature(signatureData.toString(), "HmacSHA256")
        return hmac
    }
}
