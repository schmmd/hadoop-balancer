package com.schmitztech.hadoop

import java.io.File
import scala.collection.JavaConverters._
import org.apache.commons.io.FileUtils

case class DataDirectory(val file: File) {
  require(file.getPath contains "current")
  var freeSpaceOnDisk = file.getUsableSpace()
  def humanFreeSpaceOnDisk = 
    FileUtils.byteCountToDisplaySize(freeSpaceOnDisk)

  def size = blocks.iterator.map(_.size).sum
  def humanSize = FileUtils.byteCountToDisplaySize(size)

  var blocks: Set[Block] = {
    (for {
      metaFile <- FileUtils.listFiles(file, Array("meta"), true).asScala
      DataDirectory.metaFileRegex(blockFile) = metaFile.getName
    } yield {
      Block(new File(metaFile.getParent, blockFile), metaFile)
    }).toSet
  }

  def move(block: Block, directory: DataDirectory) {
    val dest = {
      val path = block.blockFile.getParentFile.getPath
      val suffix = path.drop(path.indexOf("current"))
      val prefix = directory.file.getPath.take(path.indexOf("current"))
      new File(prefix + "/" + suffix)
    }

    println(s"Move '${block.blockFile.getPath}' to '$dest'...")

    require(!(new File(dest, block.blockFile.getName)).exists(), "destination block file already exists: " + new File(dest, block.blockFile.getName))
    require(!(new File(dest, block.metaFile.getName)).exists(), "destination meta file already exists: " + new File(dest, block.metaFile.getName))

    // record size
    val size = block.blockFile.length() + block.metaFile.length()

    // move the file
    FileUtils.moveFileToDirectory(block.blockFile, dest, true)
    FileUtils.moveFileToDirectory(block.metaFile, dest, true)

    println(s"Moved '${block.blockFile.getPath}' to '$dest'.")
    println()

    // update free space
    this.freeSpaceOnDisk += size
    directory.freeSpaceOnDisk -= size

    this.blocks -= block
    directory.blocks += block
  }
}

object DataDirectory {
  val metaFileRegex = "(blk_[+-]?\\d+)_\\d+.meta".r
}
