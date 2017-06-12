package com.recommendengine.compute.db.hbase

import java.util.HashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.hadoop.hbase.NamespaceExistException
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.jdom.Document
import org.jdom.Element
import org.jdom.input.SAXBuilder

object FileMappingToHbase {

  private val defaultMappingFile = "base-hbase-mapping.xml"

  def readMappingFile(fileName: String): java.util.Map[String, HTableDescriptor] = {

    val mappingFile = if (fileName == null) defaultMappingFile else fileName

    val build: SAXBuilder = new SAXBuilder

    println(mappingFile)

    val doc: Document = build.build(getClass.getClassLoader.getResourceAsStream(mappingFile))

    val root = doc.getRootElement

    val tables: java.util.List[Element] = root.getChildren("table").asInstanceOf[java.util.List[Element]]

    val descs = new HashMap[String, HTableDescriptor]()

    for (i <- 0 until tables.size()) {

      val table = tables.get(i)

      val tableName = table.getAttributeValue("name")

      val desc: HTableDescriptor = new HTableDescriptor(tableName)

      val keyClass = table.getAttributeValue("keyClass")

      val fields = table.getChildren("family").asInstanceOf[java.util.List[Element]]

      for (j <- 0 until fields.size()) {

        val family = fields.get(j)
        val familyName = family.getAttributeValue("name");
        val compression = family.getAttributeValue("compression");
        val blockCache = family.getAttributeValue("blockCache");
        val blockSize = family.getAttributeValue("blockSize");
        val bloomFilter = family.getAttributeValue("bloomFilter");
        val maxVersions = family.getAttributeValue("maxVersions");
        val timeToLive = family.getAttributeValue("timeToLive");
        val inMemory = family.getAttributeValue("inMemory");

        val hcol = HbaseMappingBuilder.build(tableName, familyName, compression, blockCache, blockSize, bloomFilter, maxVersions, timeToLive, inMemory)
        //  println(tableName, familyName, compression, blockCache, blockSize, bloomFilter, maxVersions, timeToLive, inMemory)

        desc.addFamily(hcol)
      }
      descs.put(tableName, desc)

    }

    descs

  }

  def createTable(desc: HTableDescriptor, conf: Configuration, nameSpace: String) {

    val name = nameSpace + ":" + Bytes.toString(desc.getName)

    val admin = new HBaseAdmin(conf)
    try {
      admin.listNamespaceDescriptors().find { x => x.getName.equals(nameSpace) } match {
        case None =>
          admin.createNamespace(NamespaceDescriptor.create(nameSpace).build())

        case Some(_) =>
      }

      if (!admin.tableExists(name)) {
        desc.setName(name.getBytes)
        admin.createTable(desc)
        admin.flush(name)

      }

    } catch {
      case e0: NamespaceExistException => {}

    } finally { admin.close() }
  }

  def deleteNameSpace(nameSpace: String, conf: Configuration, ignoreTableExit: Boolean): Unit = {

    val admin = new HBaseAdmin(conf)
    try {
      val desc = admin.getNamespaceDescriptor(nameSpace)

      val tables = admin.getTableNames()
      if (tables.length != 0 && ignoreTableExit)
        tables.foreach { x =>
          if (x.startsWith(nameSpace + ":")) {
            admin.disableTable(x)
            admin.deleteTable(x)
          }
        }

      admin.deleteNamespace(nameSpace)
    } catch {
      case e: Exception => e.printStackTrace()

    } finally {

      admin.close()
    }
  }

  def deleteTable(desc: HTableDescriptor, conf: Configuration, nameSpace: String) {

    val name = conf.get("preferred.table.name", Bytes.toString(desc.getName))

    val admin = new HBaseAdmin(conf)
    try {
      admin.createNamespace(NamespaceDescriptor.create(nameSpace).build())
    } catch {
      case e0: NamespaceExistException => {}

    } finally {
      if (!admin.tableExists(name)) {
        desc.setName(name.getBytes)
        admin.disableTable(name)
        admin.deleteTable(name)
        admin.deleteNamespace(nameSpace)
        admin.flush(name)
        admin.close()

      }

    }
  }

  //  def getOrCreateFamily(tableName:String,familyName:String,compression:String,blockCache:String,blockSize:String,
  //                bloomFilter:String,maxVersions:String,timeToLive:String,inMemory:String){
  //    
  //           val familys=HbaseMappingBuilder.getOrCreateFamilys(tableName)
  //           val columnDescriptor=HbaseMappingBuilder.getOrCreateCol(familyName, familys)
  //           
  //           if(compression != null)
  //              columnDescriptor.setCompressionType(Algorithm.valueOf(compression));
  //           if(blockCache != null)
  //              columnDescriptor.setBlockCacheEnabled(Boolean.parseBoolean(blockCache));
  //           if(blockSize != null)
  //              columnDescriptor.setBlocksize(Integer.parseInt(blockSize));
  //           if(bloomFilter != null)
  //              columnDescriptor.setBloomFilterType(BloomType.valueOf(bloomFilter));
  //           if(maxVersions != null)
  //              columnDescriptor.setMaxVersions(Integer.parseInt(maxVersions));
  //           if(timeToLive != null)
  //              columnDescriptor.setTimeToLive(Integer.parseInt(timeToLive));
  //           if(inMemory != null)
  //              columnDescriptor.setInMemory(Boolean.parseBoolean(inMemory));   
  //                
  //           
  //  }
}