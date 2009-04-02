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
 * GroovyClouds SQS Test
 */

package com.groovyclouds.aws.sqs

import com.groovyclouds.aws.SQSDriver
import com.groovyclouds.aws.exception.SQSException
import com.groovyclouds.aws.exception.ServiceException
import com.groovyclouds.aws.exception.AWSException


class SQSMain {
   private static def queue1 = 'queue1', queue2 = 'queue2'

   static void main(String[] args) {
      test(args)
   }

   private static def test(String[] args) {
      def config = new ConfigSlurper().parse(new File(args[0]).toURL())
      def sqsDriver = SQSDriver.setupInstance(config.AWS_ACCESS_KEY_ID, config.AWS_SECRET_ACCESS_KEY, null, config)

      listQueues()
      createQueue(queue1)
      createQueue(queue2, 100)
      Thread.sleep(60000)
      listQueues()

      getAttributes(queue1)
      getAttributes(queue2)

      (0..5).each { sendMessage(queue1, "life is good$it") }
      (0..5).each { sendMessage(queue2, "life is bad$it") }
      Thread.sleep(3600)

      setAttributes(queue1, ["VisibilityTimeout": 300])
      setAttributes(queue2, ["VisibilityTimeout": 500])
      Thread.sleep(3600)

      receiveMessage(queue1)
      receiveMessage(queue1)
      receiveMessage(queue1)
      receiveMessage(queue1, ["VisibilityTimeout": 1000])
      receiveMessage(queue1)
      receiveMessage(queue2)
      receiveMessage(queue2)
      receiveMessage(queue2)
      receiveMessage(queue2)
      receiveMessage(queue2, ["MaxNumberOfMessages": 10, "VisibilityTimeout": 700])

      (0..10).each { deleteMessage(queue1) }
      (0..10).each { deleteMessage(queue2) }

      deleteQueue(queue1)
      deleteQueue(queue2)
      Thread.sleep(60000)
      listQueues()
   }

   private static createQueue(queueName, defaultVisibilityTimeout = 0) {
      def sqsDriver = SQSDriver.getInstance()
      try {
         println "createQueue $queueName"
         println sqsDriver.createQueue(queueName, defaultVisibilityTimeout)
      }
      catch (AWSException e) {println e}
      catch (ServiceException e) {println e}
      catch (SQSException e) {println e}
   }

   private static deleteQueue(queueName) {
      def sqsDriver = SQSDriver.getInstance()
      try {
         println "deleteQueue $queueName"
         println sqsDriver.deleteQueue(queueName)
      }
      catch (AWSException e) {println e}
      catch (ServiceException e) {println e}
      catch (SQSException e) {println e}
   }

   private static listQueues() {
      def sqsDriver = SQSDriver.getInstance()
      try {
         println "listQueues"
         println sqsDriver.listQueues()
      }
      catch (AWSException e) {println e}
      catch (ServiceException e) {println e}
      catch (SQSException e) {println e}
   }

   private static getAttributes(queueName) {
      def sqsDriver = SQSDriver.getInstance()
      try {
         println "getQueueAttributes $queueName"
         println sqsDriver.getQueueAttributes(queueName)
      }
      catch (AWSException e) {println e}
      catch (ServiceException e) {println e}
      catch (SQSException e) {println e}
   }

   private static setAttributes(queueName, attributes) {
      def sqsDriver = SQSDriver.getInstance()
      try {
         println "setAttributes $queueName"
         println sqsDriver.setQueueAttributes(queueName, attributes)
      }
      catch (AWSException e) {println e}
      catch (ServiceException e) {println e}
      catch (SQSException e) {println e}
   }

   private static sendMessage(queueName, message) {
      def sqsDriver = SQSDriver.getInstance()
      try {
         println "sendMessage $queueName"
         println sqsDriver.sendMessage(queueName, message)
      }
      catch (AWSException e) {println e}
      catch (ServiceException e) {println e}
      catch (SQSException e) {println e}
   }

   private static receiveMessage(queueName, attributes = null) {
      def sqsDriver = SQSDriver.getInstance()
      try {
         println "receiveMessage $queueName"
         println sqsDriver.receiveMessage(queueName, attributes)
      }
      catch (AWSException e) {println e}
      catch (ServiceException e) {println e}
      catch (SQSException e) {println e}
   }

   private static deleteMessage(queueName) {
      def sqsDriver = SQSDriver.getInstance()
      try {
         println "receiveMessage $queueName"
         def res = sqsDriver.receiveMessage(queueName)
         if (res) {
            println "deleteMessage from $queueName message " + res[0].get("ReceiptHandle")
            println sqsDriver.deleteMessage(queueName, res[0].get("ReceiptHandle"))
         }
      }
      catch (AWSException e) {println e}
      catch (ServiceException e) {println e}
      catch (SQSException e) {println e}
   }
}