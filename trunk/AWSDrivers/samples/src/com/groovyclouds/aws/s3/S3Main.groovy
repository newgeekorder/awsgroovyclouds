/**
 *
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
 * GroovyClouds S3 Test
 */

package com.groovyclouds.aws.s3

import com.groovyclouds.aws.S3Driver
import com.groovyclouds.aws.S3Grant
import com.groovyclouds.aws.S3CopySource
import com.groovyclouds.aws.S3LoggingStatus


class S3Main {
    static def config
    static def email_1 = "yourname@gmail.com"
    static def email_2 = "yourname@yahoo.com"
    static def filename = "/Users/test/test.txt"
   
    public static void main(String[] args)   {

        config = new ConfigSlurper().parse(new File(args[0]).toURL())
       // testRE()

        S3Driver s3= S3Driver.setupInstance(config.AWS_ACCESS_KEY_ID, config.AWS_SECRET_ACCESS_KEY, "", config)

                
        s3.listBuckets()
        s3.createBucket("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase())
        s3.createBucket("${config.AWS_ACCESS_KEY_ID}videos".toString())

        s3.setRequestPayment("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase(), S3Driver.PAYER_REQUESTER)
        s3.getRequestPayment("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase())
        s3.bucketExists("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase())
        s3.bucketExists("${config.AWS_ACCESS_KEY_ID}videos".toString().toLowerCase())
        s3.bucketExists("${config.AWS_ACCESS_KEY_ID}photos".toString())

        s3.addTextObject("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase(), "summer/1998", "hello 1998 summer 'fall",
                                                                       ["test": "yes", "dated": "no"])

        s3.getObjectInfo("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase(),"summer/1998")
        s3.listKeys("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase())
        s3.listKeys("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase(), "summer/")
        s3.getBucketLocation("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase())
        s3.getTextObject("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase(), "summer/1998")
        s3.listKeys("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase())
        s3.getBucketACL("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase())
        s3.getObjectACL("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase(), "summer/1998")


        /*
            acl.type = grant.Grantee.@type.text()
            acl.ID = grant.Grantee.ID.text()
            acl.DisplayName = grant.Grantee.DisplayName.text()
            acl.URI = grant.URI.text()
            acl.Permission = grant.Permission.text()
        */
       S3Grant.toXML("myname", "abc",
                [[type:S3Grant.GRANTEE_GROUP, Permission:S3Grant.PERMISSION_READ, groupType:S3Grant.GROUP_ALL_USERS],
                    [type:S3Grant.GRANTEE_CANONICAL_USER, Permission:S3Grant.PERMISSION_WRITE, ID:"123456", DisplayName:""],
                    [type:S3Grant.GRANTEE_CUSTOMER_EMAIL, Permission:S3Grant.PERMISSION_WRITE_ACP, EmailAddress:email_1]])
       s3.getBucketACL("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase())

       s3.setBucketACL("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase(), "e8197e0af4be4c1c1f2a8d6c4b5e2cb1a62b5ba5512c4d748b8343f2e39878fe", "yourname",
                    [[type:S3Grant.GRANTEE_GROUP, Permission:S3Grant.PERMISSION_READ, groupType:S3Grant.GROUP_ALL_USERS],
                     [type:S3Grant.GRANTEE_CANONICAL_USER, Permission:S3Grant.PERMISSION_FULL_CONTROL, ID:"e8197e0af4be4c1c1f2a8d6c4b5e2cb1a62b5ba5512c4d748b8343f2e39878fe", DisplayName:"Your Name"],
                    [type:S3Grant.GRANTEE_CUSTOMER_EMAIL, Permission:S3Grant.PERMISSION_WRITE_ACP, EmailAddress:email_1]])
       s3.setObjectACL("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase(), "summer/1998", "e8197e0af4be4c1c1f2a8d6c4b5e2cb1a62b5ba5512c4d748b8343f2e39878fe", "your_name",
                            [[type:S3Grant.GRANTEE_GROUP, Permission:S3Grant.PERMISSION_READ, groupType:S3Grant.GROUP_ALL_USERS],
                             [type:S3Grant.GRANTEE_CANONICAL_USER, Permission:S3Grant.PERMISSION_FULL_CONTROL, ID:"e8197e0af4be4c1c1f2a8d6c4b5e2cb1a62b5ba5512c4d748b8343f2e39878fe", DisplayName:"your_name"],
                            [type:S3Grant.GRANTEE_CUSTOMER_EMAIL, Permission:S3Grant.PERMISSION_WRITE_ACP, EmailAddress:email_1]])

        s3.getObjectACL("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase(), "summer/1998")


        s3.addTextObject("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase(), "summer/2009-June", "Here are some photos taken in a beach", [title:"best pics"],
        S3Driver.TYPE_PLAIN_TEXT, S3Driver.DEFAULT_ENCODING, S3Grant.ACL_PUBLIC_READ)

        s3.getTextObject("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase(), "summer/2009-June")

        s3.addFileObject("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase(), "summer/files/text.txt", filename, "text/plain")
        s3.getFileObject("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase(), "summer/files/text.txt")
        def s3src = new S3CopySource("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase() + "/summer/2009-June")
        System.out.println(s3src.toString())
        System.out.println(s3src.getHeaders())
        
        s3.copyObject("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase(), "winter/2009-June", s3src)
        s3.deleteObject("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase(), "summer/1998")

        loggingStatus()
        def (owner, buckets) = s3.listBuckets()       
        buckets.each {  bucket ->
         try   {
            s3.deleteBucket(bucket.Name)
         }
         catch (Exception e){}
        }
    }

    static def loggingStatus()  {
        try {
            def s3= S3Driver.getInstance("me")
            s3.setBucketLoggingStatus("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase(),
                    new S3LoggingStatus(targetBucket:"${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase(),
                                        targetPrefix:"server-log-", targetGrants:[[type:'AmazonCustomerByEmail', EmailAddress:email_2, Permission:S3Grant.PERMISSION_WRITE]]))

            s3.getBucketLoggingStatus("${config.AWS_ACCESS_KEY_ID}photos".toString().toLowerCase())
        }
        catch (Exception e) {
            e.printStackTrace()
        }

    }
    static def testRE() {
        StringBuilder strbuf = new StringBuilder()
        def path = "/?requestPayment"
       if (path?.matches(/.*[&\?]requestPayment(=|&|$).*/))   {
            strbuf.append('?requestPayment')
        }
        path= "/?acl"
        if (path?.matches(/.*[&\?]acl(=|&|$).*/))   {
            strbuf.append('?acl')
        }


        System.out.println("testre:"+ strbuf.toString())
    }
    static def testDNSName()    {
        def name = "abc-.bc.109a"
        System.out.println("${name} : ${S3Driver.isDNSName(name)}")
        name = "ABC.bc.109a"
        System.out.println("${name} : ${S3Driver.isDNSName(name)}")
        name = "!abc-.bc.109a"
        System.out.println("${name} : ${S3Driver.isDNSName(name)}")
        name = "abc.bc.109a"
        System.out.println("${name} : ${S3Driver.isDNSName(name)}")
        name = "192.168.1.2"
        System.out.println("${name} : ${S3Driver.isDNSName(name)}")
    }
}