package model.paytv

case class PaytvResponse(
                         statusContract: Array[((String,String,String),Long)],
                         nameContract: Array[((String,String),Long)],
                         provinceContract: Array[((String,String),Long)]
                       )