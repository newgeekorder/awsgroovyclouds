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
 * GroovyClouds SDB Test
 */

package com.groovyclouds.aws.sdb

import com.groovyclouds.aws.SDBDriver

public class SDBMain {
   private static def table1 = 'media', table2 = 'address'
   private static sampleQuerySet = [
           [ItemName: '0385333498', Title: 'The Sirens of Titan',
                   Author: "Kurt Vonnegut", Year: '1959', Pages: 336,
                   Keyword: ['Book', 'Paperback'], Rating: ['*****', '5 stars', 'Excellent']],

           [ItemName: '0802131786', Title: 'Tropic of Cancer',
                   Author: "Henry Miller", Year: '1934', Pages: 318,
                   Keyword: ['Book'], Rating: ['****']],

           [ItemName: '1579124585', Title: 'The Right Stuff',
                   Author: "Tom Wolfe", Year: '1979', Pages: 304,
                   Keyword: ['Book', 'Hardcover', 'American'], Rating: ['****', '4 stars']],

           [ItemName: 'B000T9886K', Title: 'In Between',
                   Author: "Paul Van Dyk", Year: '2007',
                   Keyword: ['CD', 'Trance'], Rating: ['4 stars']],

           [ItemName: 'B00005JPLW', Title: '300',
                   Author: "Zack Snyder", Year: '2007',
                   Keyword: ['DVD', 'Action', 'Frank Miller'], Rating: ['***', '3 stars', 'Not bad']],

           [ItemName: 'B000SF3NGK', Title: 'Heaven\'s Gonna Burn Your Eyes',
                   Author: "Thievery Corporation", Year: '2002', Rating: ['*****']],

           [ItemName: 'ATTR35', attr1: "attr1/attr2", attr2: "attr2", attr3: "attr3", attr4: "attr4", attr5: "attr5",
                   attr6: "attr6", attr7: "attr7", attr8: "attr8", attr9: "attr9", attr10: "attr10",
                   attr11: "attr11", attr12: "attr12", attr13: "attr13", attr14: "attr14", attr15: "attr15",
                   attr16: "attr16", attr17: "attr17", attr18: "attr18", attr19: "attr19", attr20: "attr20",
                   attr21: "attr21", attr22: "attr22", attr23: "attr23", attr24: "attr24", attr25: "attr25",
                   attr26: "attr26", attr27: "attr27", attr28: "attr28", attr29: "attr29", attr30: "attr30",
                   attr31: "attr31", attr32: "attr32", attr33: "attr33", attr34: "attr34", attr35: "attr35"]
   ]
   private static def config

   public static void main(String[] args) {
        config = new ConfigSlurper().parse(new File(args[0]).toURL())
       /*config = new java.util.Properties()
       config.load(new FileInputStream(args[0]))*/
      test1()
      test2()
   }

   private static def test1() {
      SDBDriver sdbdriver = SDBDriver.setupInstance(config.AWS_ACCESS_KEY_ID, config.AWS_SECRET_ACCESS_KEY, null, config)
      createDomain()
      getMetaData()
      Thread.sleep(60000)
      System.out.println(sdbdriver.listDomains())
      addSampleSet()
      replaceSample()
      selectSampleSet()
      querySampleSet()
      queryAttributesSampleSet()
      deleteDomain()
      deleteAllDomains()
   }

   private static failCreateDomain(config) {
      def sdbdriver = SDBDriver.getInstance()
      System.out.println(sdbdriver.listDomains())
      try {

         System.out.println(sdbdriver.createDomain(table1))
      }
      catch (Exception e) {}

   }

   private static addSampleSet() {
      def sdbdriver = SDBDriver.getInstance()
      def ret
      sampleQuerySet.each {item ->
         ret = sdbdriver.setItem(table1, item.ItemName, item)
         System.out.println("setItem returns $ret for ${item.ItemName}")
      }

   }

   private static replaceSample() {
      def sdbdriver = SDBDriver.getInstance()
      def item = [ItemName: 'ATTR35', attr1: "attr new", attr2: "attr2", attr3: "attr3", attr4: "attr4", attr5: "attr5",
              attr6: "attr6", attr7: "attr7", attr8: "attr8", attr9: "attr9", attr10: "attr10",
              attr11: "attr11", attr12: "attr12", attr13: "attr13", attr14: "attr14", attr15: "attr15",
              attr16: "attr16", attr17: "attr17", attr18: "attr18", attr19: "attr19", attr20: "attr20",
              attr21: "attr21", attr22: "attr22", attr23: "attr23", attr24: "attr24", attr25: "attr25",
              attr26: "attr26", attr27: "attr27", attr28: "attr28", attr29: "attr29", attr30: "attr30",
              attr31: "attr31 new", attr32: "attr32 new", attr33: "attr33", attr34: "attr34", attr35: "attr35"]
      def ret = sdbdriver.setItem(table1, item.ItemName, item, "true")
      System.out.println("setItem returns $ret for ${item.ItemName}")
      ret = sdbdriver.getItem(table1, 'ATTR35')
      System.out.println("getItem returns ${item.ItemName} is $ret")
   }

   private static selectSampleSet() {
      SDBDriver sdbdriver = SDBDriver.getInstance()
      System.out.println(sdbdriver.select("select ItemName from ${table1}".toString()))
      System.out.println(sdbdriver.select("select ItemName, Rating from ${table1}".toString()))

      System.out.println(sdbdriver.select("select * from ${table1} where Title = 'The Right Stuff'".toString()))
      System.out.println(sdbdriver.select("select * from ${table1} where Year > '1985'".toString()))
      System.out.println(sdbdriver.select("select * from ${table1} where Rating like '****%'".toString()))
      System.out.println(sdbdriver.select("select * from ${table1} where Pages < '00320'".toString()))

      System.out.println(sdbdriver.select("select * from ${table1} where Rating = '****'".toString()))
      System.out.println(sdbdriver.select("select * from ${table1} where every(Rating) = '****'".toString()))
      System.out.println(sdbdriver.select("select * from ${table1} where Keyword = 'Book' intersection Keyword = 'Hardcover'".toString()))


      System.out.println(sdbdriver.select("select count(*) from ${table1} limit 500".toString()))
      System.out.println(sdbdriver.select("select count(*) from ${table1} where Year > '1985'".toString()))
      System.out.println(sdbdriver.select("select count(*) from ${table1}".toString()))
      def expr = sdbdriver.buildQueryExpression("select Pages, Year, Rating from \${table1} where Pages < '\${pages}'".toString(), [table1: 'media', pages: 320])
      System.out.println(sdbdriver.select(expr))

   }

   private static querySampleSet() {
      SDBDriver sdbdriver = SDBDriver.getInstance()
      System.out.println(sdbdriver.query('media', "['Title' = 'The Right Stuff']"))
      System.out.println(sdbdriver.query('media', "['Year' > '1985']"))
      System.out.println(sdbdriver.query('media', "['Rating' starts-with '****']"))
      System.out.println(sdbdriver.query('media', "['Year' > '1975' and 'Year' < '2008']"))
      System.out.println(sdbdriver.query('media', "['Rating' = '***' or 'Rating' = '*****']"))
      System.out.println(sdbdriver.query('media', "['Year' > '1950' and 'Year' < '1960' or 'Year' starts-with '193' or 'Year' = '2007']"))
      System.out.println(sdbdriver.query('media', "['Rating' = '4 stars' or 'Rating' = '****']"))
      System.out.println(sdbdriver.query('media', "['Keyword' = 'Book' and 'Keyword' = 'Hardcover']"))
      System.out.println(sdbdriver.query('media', "['Keyword' != 'Book']"))
      System.out.println(sdbdriver.query('media', "not['Keyword' = 'Book']"))
      System.out.println(sdbdriver.query('media', "['Keyword' = 'Frank Miller'] union ['Rating' starts-with '****']"))
      System.out.println(sdbdriver.query('media', "['Year' >= '1900' and 'Year' < '2000'] intersection ['Keyword' = 'Book'] intersection ['Rating' starts-with '4' or 'Rating' = '****'] union ['Title' = '300'] union ['Author' = 'Paul Van Dyk']"))
      System.out.println(sdbdriver.query('media', "['Year' < '1980'] sort 'Year' asc"))
      System.out.println(sdbdriver.query('media', "['Year' = '2007'] intersection ['Author' starts-with ''] sort 'Author' desc"))
      def expr = sdbdriver.buildQueryExpression("['Pages' < '\${pages}']", [pages: 320])
      System.out.println(sdbdriver.query('media', expr))
   }

   private static queryAttributesSampleSet() {
      SDBDriver sdbdriver = SDBDriver.getInstance()
      System.out.println(sdbdriver.queryWithAttributes('media', "['Title' = 'The Right Stuff']", ['Author']))
      System.out.println(sdbdriver.queryWithAttributes('media', "['Year' > '1985']", ['Author', 'Year', 'Rating']))
      System.out.println(sdbdriver.queryWithAttributes('media', "['Rating' starts-with '****']"))
      System.out.println(sdbdriver.queryWithAttributes('media', "['Year' > '1975' and 'Year' < '2008']"))
      System.out.println(sdbdriver.queryWithAttributes('media', "['Rating' = '***' or 'Rating' = '*****']"))
      System.out.println(sdbdriver.queryWithAttributes('media', "['Year' > '1950' and 'Year' < '1960' or 'Year' starts-with '193' or 'Year' = '2007']"))
      System.out.println(sdbdriver.queryWithAttributes('media', "['Rating' = '4 stars' or 'Rating' = '****']"))
      System.out.println(sdbdriver.queryWithAttributes('media', "['Keyword' = 'Book' and 'Keyword' = 'Hardcover']"))
      System.out.println(sdbdriver.queryWithAttributes('media', "['Keyword' != 'Book']"))
      System.out.println(sdbdriver.queryWithAttributes('media', "not['Keyword' = 'Book']"))
      System.out.println(sdbdriver.queryWithAttributes('media', "['Keyword' = 'Frank Miller'] union ['Rating' starts-with '****']"))
      System.out.println(sdbdriver.queryWithAttributes('media', "['Year' >= '1900' and 'Year' < '2000'] intersection ['Keyword' = 'Book'] intersection ['Rating' starts-with '4' or 'Rating' = '****'] union ['Title' = '300'] union ['Author' = 'Paul Van Dyk']"))
      System.out.println(sdbdriver.queryWithAttributes('media', "['Year' < '1980'] sort 'Year' asc"))
      System.out.println(sdbdriver.queryWithAttributes('media', "['Year' = '2007'] intersection ['Author' starts-with ''] sort 'Author' desc"))
      def expr = sdbdriver.buildQueryExpression("['Pages' < '\${pages}']", [pages: 320])
      System.out.println(sdbdriver.queryWithAttributes('media', expr, ['Author', 'Year', 'Rating']))
   }

   private static createDomain() {
      def sdbdriver = SDBDriver.getInstance()
      try {
         System.out.println("Create domain $table1")
         System.out.println(sdbdriver.createDomain(table1))
      }
      catch (Exception e) {}
   }

   private static getMetaData() {
      def sdbdriver = SDBDriver.getInstance()
      try {
         System.out.println("Domain Media metadata  ${sdbdriver.getDomainMetadata(table1)}")
      }
      catch (Exception e) {}

   }

   private static deleteDomain() {
      def sdbdriver = SDBDriver.getInstance()
      try {
         System.out.println("Delete domain $table1")
         System.out.println(sdbdriver.deleteDomain(table1))
      }
      catch (Exception e) {}
      Thread.sleep(20000)
      System.out.println(sdbdriver.listDomains())
   }

   private static deleteAllDomains() {
      def sdbdriver = SDBDriver.getInstance()
      try {
         def domains = sdbdriver.listDomains()
         domains.each {
            System.out.println("Delete domain ${it}")
            System.out.println(sdbdriver.deleteDomain(it))
         }
      }
      catch (Exception e) {}
      Thread.sleep(20000)
   }

   private static test2() {
      SDBDriver.setupInstance(config.AWS_ACCESS_KEY_ID, config.AWS_SECRET_ACCESS_KEY, "myschema", config)
      batchPutAttributes()
   }

   private static batchPutAttributes() {
      def sdbdriver = SDBDriver.getInstance("myschema")
      sdbdriver.createDomain(table2)
      def ret = sdbdriver.setItem(table2, [
              'id-1': [street: '100 M Circle', city: "San Francisco", state: 'CA'],
              'id-2': [street: '2304 Washington Ave', city: "Houston", state: 'TX'],
              'id-3': [street: "#123,Redwood Circle", city: "Phoenix", state: 'AZ'],
              'id-4': [attr1: "attr1", attr2: "attr2", attr3: "attr3", attr4: "attr4", attr5: "attr5",
                      attr6: "attr6", attr7: "attr7", attr8: "attr8", attr9: "attr9", attr10: "attr10",
                      attr11: "attr11", attr12: "attr12", attr13: "attr13", attr14: "attr14", attr15: "attr15",
                      attr16: "attr16", attr17: "attr17", attr18: "attr18", attr19: "attr19", attr20: "attr20",
                      attr21: "attr21", attr22: "attr22", attr23: "attr23", attr24: "attr24", attr25: "attr25",
                      attr26: "attr26", attr27: "attr27", attr28: "attr28", attr29: "attr29", attr30: "attr30",
                      attr31: "attr31", attr32: "attr32", attr33: "attr33", attr34: "attr34", attr35: "attr35"]
      ])
      ret = sdbdriver.getItem(table2, 'id-1')
      System.out.println("getItem returns $ret")
      ret = sdbdriver.deleteItem(table2, 'id-1')
      System.out.println("deleteItem returns $ret")
      Thread.sleep(20000)
      ret = sdbdriver.getItem(table2, 'id-4')
      System.out.println("getItem returns $ret")
      sdbdriver.listDomains()
      println sdbdriver.domainExists("books")
      println sdbdriver.domainExists(table2)
      println sdbdriver.deleteDomain(table2)
   }
}