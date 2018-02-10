package org.apache.spark.coalescer

import org.apache.spark.Partition
import org.apache.spark.rdd.{PartitionCoalescer, PartitionGroup, RDD}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class MyCoalescer extends PartitionCoalescer {

  val locationPart = new mutable.HashMap[String, mutable.Set[Partition]]()
  val partNumInLocs = new mutable.HashMap[String, Int]()

  val groupArr = ArrayBuffer[PartitionGroup]()

  // gets the *current* preferred locations from the DAGScheduler (as opposed to the static ones)
  def currPrefLocs(part: Partition, prev: RDD[_]): Seq[String] = {
    prev.context.getPreferredLocs(prev, part.index).map(tl => tl.host)
  }

  def getPartitions: Array[PartitionGroup] = groupArr.filter(pg => pg.numPartitions > 0).toArray

  class PartitionLocations(prev: RDD[_]) {

    // contains all the partitions from the previous RDD that don't have preferred locations
    val partsWithoutLocs = ArrayBuffer[Partition]()
    // contains all the partitions from the previous RDD that have preferred locations
    val partsWithLocs = ArrayBuffer[(String, Partition)]()

    getAllPrefLocs(prev)

    // gets all the preferred locations of the previous RDD and splits them into partitions
    // with preferred locations and ones without
    def getAllPrefLocs(prev: RDD[_]): Unit = {
      val tmpPartsWithLocs = mutable.LinkedHashMap[Partition, Seq[String]]()
      // first get the locations for each partition, only do this once since it can be expensive
      prev.partitions.foreach(p => {
        val locs = currPrefLocs(p, prev)
        if (locs.nonEmpty) {
          tmpPartsWithLocs.put(p, locs)
        } else {
          partsWithoutLocs += p
        }
      }
      )
      // convert it into an array of host to partition
      for (x <- 0 to 2) {
        tmpPartsWithLocs.foreach { parts =>
          val p = parts._1
          val locs = parts._2
          if (locs.size > x) partsWithLocs += ((locs(x), p))
        }
      }
    }
  }

  def setupPartitionGroup(partLocs: PartitionLocations) = {
    val locGroup = new mutable.HashMap[String, PartitionGroup]()

    var targetLen = 0
    partLocs.partsWithLocs.foreach { pl =>
      val (loc, part) = pl
      locationPart.getOrElseUpdate(loc, mutable.Set.empty[Partition]) += part
      partNumInLocs.get(loc) match {
        case None => {
          partNumInLocs.update(loc, 1)
          locGroup.update(loc, new PartitionGroup(Some(loc)))
          groupArr += new PartitionGroup(Some(loc))
          targetLen += 1
        }
        case Some(count) => partNumInLocs.update(loc, count + 1)
      }
    }

    val (maxLoc, maxPart) = partNumInLocs.maxBy(_._2)
    balancePartition(maxLoc, locGroup)
  }

  def balancePartition(loc: String, locGroup: mutable.Map[String, PartitionGroup]) = {
    val partSet = locationPart(loc)
    val part = partSet.last
    partSet.remove(part)
    locGroup(loc).partitions += part

    locationPart.foreach { lp =>
      val (loc, parts) = lp
      if (parts.remove(part)) {
        val count = partNumInLocs(loc)
        if (count > 0)
          partNumInLocs.update(loc, count - 1)
        else {
          locationPart.remove(loc)
          partNumInLocs.remove(loc)
        }
      }
    }
  }

  override def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup] = {
    val partLocs = new PartitionLocations(parent)
    setupPartitionGroup(partLocs)
    getPartitions
  }
}
