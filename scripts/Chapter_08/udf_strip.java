package com.packtpub.hive.essentials.hiveudf ;
 
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;

@Description(
	name = "udf_strip", 
	value = "_FUNC_(x, n) - Strip the string n from x", 
	extended = "This will be result returned by the explain extended statement."
)
@UDFType(deterministic = true, stateful = false)
public class udf_strip extends UDF {
   public Text evaluate(String str) {
      return str == null ? null : new Text(StringUtils.strip(str));
   }
   public Text evaluate(String str,String chrStr) {
      return str == null ? null : new Text(StringUtils.strip(str,chrStr));
   }
}