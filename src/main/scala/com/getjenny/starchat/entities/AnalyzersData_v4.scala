package com.getjenny.starchat.entities

/**
  * Created by angelo on 21/07/19.
  */

case class AnalyzersData_v4(
                 item_list: Vector[String] = Vector.empty[String],
                 extracted_variables: Map[String, String] = Map.empty[String, String]
               )
