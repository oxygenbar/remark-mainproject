package cn.oxygenbar.util;


import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;




public class StringUtil  {
	public static Float toMoney(String s){
		if (s==null) return 0f;
		if (s.indexOf(".")!=-1){
			
			if(s.indexOf(".") == 0){
				s = "0" + s;
			}
			 
			s += "00"; 
			s = s.substring(0, s.indexOf(".")+3);
		}
		Float fl = toFloat(s);
		if (fl == null) fl = 0f;
		return fl;
	} 
	
	
	public static Float toFloat(String s){

		return toFloat(s , null);
	}
	
	
	public static Double toDouble(String s){
		return toDouble(s , null);
	}
	public static Float toFloat(String s,Float defaultValue){
		if (s==null||"".equals(s.trim())||"Infinity".equals(s.trim())||"NaN".equals(s.trim()))
			return defaultValue;
		try {
			return Float.parseFloat(s.trim()); 
		} catch(Exception e) {
			Logs.geterrorLogger().error(e);
			return defaultValue; 
		}
	}
	public static Double toDouble(String s,Double defaultValue){
		  
		if (s==null||"".equals(s.trim())||"Infinity".equals(s.trim())||"NaN".equals(s.trim()))
			return defaultValue;
		try {
			return Double.parseDouble(s.trim());
		} catch(Exception e) {
			Logs.geterrorLogger().error(e);
			return defaultValue; 
		}
	}

	public static Integer toInteger(String s){
		return toInteger(s , 0);
	}
	
	public static Integer toInteger(String s,Integer defaultValue){
		if (s==null||"".equals(s.trim())||!s.matches("^[-+]?[0-9]+$"))
			return defaultValue;
		try {
			return Integer.parseInt(s.trim()); 
		}catch(Exception e) {
			Logs.geterrorLogger().error(e);
			return defaultValue; 
		}
	}
	public static Long toLong(String s){
		return toLong(s , null);
	}
	
	public static Long toLong(String s,Long defaultValue){
		if (s==null||"".equals(s.trim()))
			return defaultValue;
		try {
			return Long.parseLong(s.trim()); 
		}catch(Exception e) {
			Logs.geterrorLogger().error(e);
			return defaultValue; 
		}
	}
	
	public static boolean isInt(String id) {
		if(id == null) 
			return false; 
		try {
			Integer.parseInt(id.trim()); 
		} catch(Exception e) {
			Logs.geterrorLogger().error(e);
			return false; 
		}
		return true; 
	}
	public static boolean isId(String id) {
		if(id == null) 
			return false; 
		try {
			int intValue = Integer.parseInt(id.trim()); 
			if(intValue <= 0)
				return false; 
		} catch(Exception e) {
			Logs.geterrorLogger().error(e);
			return false; 
		}
		return true; 
	}	

	
	public static boolean isEmpty(String str) {
		if(str == null) 
			return true; 
		String tempStr = str.trim(); 
		if(tempStr.length() == 0)
			return true; 
		if(tempStr.equals("null"))
			return true;
		return false; 
	}
    
    /**
     * 输入字符串不是null，且包含非空格字符时返回true。
     * 
     * @param str
     * @return
     */
    public static boolean isNotBlank(String str) {
        return str != null && str.trim().length() != 0;
    }

	public static boolean isIndicator(String str) {
		if(str == null) 
			return false; 
		String tempStr = str.trim(); 
		if(tempStr.length() != 1)
			return false; 
		return true; 
	}
	
	
	public static String convertStringArrayToStringWithParser(String []inputStringArray, String parser) {
		if(inputStringArray == null || parser == null)
			return null; 
		StringBuffer buffer = new StringBuffer();
		for(int i=0; i<inputStringArray.length; i++) {
			buffer.append(inputStringArray[i]).append(parser); 
		}
		int bufferLength = buffer.length(); 
		StringBuffer newBuffer = buffer.deleteCharAt(bufferLength-1); 
		return newBuffer.toString(); 
	}	
	
	public static List<String> parseStringToList(String inputString, String parserString) {
		if (inputString == null)
			return null;

		StringTokenizer st = new StringTokenizer(inputString, parserString);
		ArrayList<String> result = new ArrayList<String>(20);

		while (st.hasMoreTokens())
			result.add(st.nextToken().trim());

		return result;
	}
	
	public static Set<String> parseStringToSet(String inputString, String parserString) {
		if (inputString == null)
			return null;

		StringTokenizer st = new StringTokenizer(inputString, parserString);
		Set<String> set = new HashSet<String>(10);

		while (st.hasMoreTokens()) {
			set.add(st.nextToken().trim());
		}
		return set;
	}
	
	public static String getFirstItemString(String inputString, String parserString) {
		if (inputString == null)
			return null;

		String result = null;
		StringTokenizer st = new StringTokenizer(inputString, parserString);

		if (st.hasMoreTokens())
			result = st.nextToken().trim();

		return result;
	}

	public static String getLastItemString(String inputString, String parserString) {
		int lastIndex = inputString.lastIndexOf(parserString);

		return inputString.substring(lastIndex + parserString.length());
	}

	public static String removeFirstItemFromString(String inputString, String parserString) {
		int index = inputString.indexOf(parserString);
		if (index < 0)
			return null;
		else
			return inputString.substring(index + parserString.length());
	}

	public static boolean isPatternInString(String targetString, String stringPattern) {
		if (targetString == null || stringPattern == null)
			return false;

		if (targetString.toLowerCase().indexOf(stringPattern.toLowerCase()) < 0)
			return false;

		return true;
	}
	
	public static String replace(String str, String pattern, String replace) {
	    int s = 0;
	    int e = 0;
	    StringBuffer result = new StringBuffer();
	
	    while ((e = str.indexOf(pattern, s)) >= 0) {
	        result.append(str.substring(s, e));
	        result.append(replace);
	        s = e+pattern.length();
	    }
	    result.append(str.substring(s));
	    return result.toString();
	}	

	public static String generateRandomString( int length, int seed ) {
		StringBuffer sb = new StringBuffer();
		Random r = new Random( System.currentTimeMillis() + seed );
		int i = 0;
		while( i++ < length ) {
			int randomNumber = r.nextInt( 62 );
			if( randomNumber >=0 && randomNumber <=9 ) {
				sb.append( (char)(randomNumber + 48) );
			} else if( randomNumber >= 10 && randomNumber <= 35 ) {
				sb.append( (char)(randomNumber + 55) );
			} else if( randomNumber >= 36 && randomNumber <= 61 ) {
				sb.append( (char)(randomNumber + 61) );
			}
		}
		return sb.toString();
	}

    public static String getIdString(int[] idArray) {
    	StringBuffer buffer = new StringBuffer(idArray.length*2); 
    	for(int i=0; i<idArray.length; i++) {
    		buffer.append(idArray[i]).append(",");  
    	}
    	int length = buffer.length(); 
    	int lastCommaIndex = buffer.lastIndexOf(","); 
    	if(length == (lastCommaIndex+1))
    		return buffer.substring(0, length-1); 
    	else
    		return buffer.toString(); 
    }

	public static String removeWhiteSpace(String str) {
		String res = null;
		
		if( str != null ) {
			char[] chars = str.toCharArray();
			res = "";
			
			for( int i = 0; i < chars.length; ++i ) {
				if( !Character.isWhitespace(chars[i]) ) {
					res += chars[i];
				}
			}
		}
		
		return res;
	}
	

	public static String normalizePhoneNumber( String phoneNumber ) {
		// strip out anything that's not 0-9
		String phone = phoneNumber.replaceAll( "[^0-9]", "" );
		phone = phone.replaceAll( "[\\s]", "" );
//		if( !phone.startsWith( "1" ) && !phone.startsWith( "0" ) ) {
//			phone = "1" + phone;
//		}
//		if(phone.length()>10 && (phone.startsWith("1") || phone.startsWith("0")))
//			phone = phone.substring(1); 
		return phone;
	}
	
	


	public static String getString(String s){
		if(isEmpty(s)){
			return "";
		}else{
			return s.trim();
		}
	}
	
	public static String utf8Togb2312(String str){
		String gbk = "";
		try {
			String utf8 = new String(str.getBytes("UTF-8"));
			String unicode = new String(utf8.getBytes(),"UTF-8");   
			gbk = new String(unicode.getBytes("GBK"));  
		} catch (UnsupportedEncodingException e) {
			Logs.geterrorLogger().error(" StringUtil utf8Togb2312 ",e );
		}  
		return gbk;
	}
	
	
	 public static String utf8togb2312(String str){   
	        StringBuffer sb = new StringBuffer();   
	        for ( int i=0; i<str.length(); i++) {   
	            char c = str.charAt(i);   
	            switch (c) {   
	               case '+' :   
	                   sb.append( ' ' );   
	               break ;   
	               case '%' :   
	                   try {   
	                        sb.append(( char )Integer.parseInt (   
	                        str.substring(i+1,i+3),16));   
	                   }   
	                   catch (NumberFormatException e) {   
	                       throw new IllegalArgumentException();   
	                  }   
	                  i += 2;   
	                  break ;   
	               default :   
	                  sb.append(c);   
	                  break ;   
	             }   
	        }   
	        String result = sb.toString();   
	        String res= null ;   
	        try {   
	             byte [] inputBytes = result.getBytes( "8859_1" );   
	            res= new String(inputBytes, "UTF-8" );   
	        }   
	        catch (Exception e){}   
	        return res;   
	  }  

	public static String subNickName(String name){
		String nameStr = "";
		if(name.length() > 2){
			nameStr = name.substring(0,1)+"**"+name.substring(name.length()-1);
		}else{
			nameStr = !StringUtil.isEmpty(name)?name.substring(0,1)+"**":"**";
		}
		return nameStr;
	}
    public static String toUTF8(String str){
        try
        {
                if(str==null)
                        str = "";
                else{
                	str=str.trim();
                	//str=toHtmlInput(str);
                	str=new String(str.getBytes("ISO-8859-1"),"utf-8");
                }
                
        }
        catch (Exception e) {
        	//com.wantuan.util.LogUtil.dealWith(ex,StringUtil.class);
        	Logs.geterrorLogger().error(e.getMessage(),e); 
        }
        return str;
    }
    
    public static String togbk(String str){
        try
        {
                if(str==null)
                        str = "";
                else{
                	str=str.trim(); 
                	str = new String(str.getBytes("gbk"),"gbk");
                }
        }
        catch (Exception e) {
        	//com.wantuan.util.LogUtil.dealWith(ex,StringUtil.class);
        	Logs.geterrorLogger().error(e.getMessage(),e); 
        }
        return str;
    }
     
    
    /*分割字符串*/
    public static String[] splitStr(String str,char c)
    {
    	try {
    		str+=c;
    		int n=0;
    		for(int i=0;i<str.length();i++)
    		{
    			if(str.charAt(i)==c)n++;
    		}
    		
    		String out[] = new String[n];
    		
    		for(int i=0;i<n;i++)
    		{
    			int index = str.indexOf(c);
    			out[i] = str.substring(0,index);
    			str = str.substring(index+1,str.length());
    		}
    		return out;
			
		} catch (Exception e) {
		}
		return null;
    }
    
    public static String UrlDecoder(String url){
    	try {
    		if(url!=null){
    			return URLDecoder.decode(url,"UTF-8");
    		}else{
    			return null;
    		}
    		
		}catch(IllegalArgumentException e){
			try{
				return URLDecoder.decode(url,"GBK");
			}catch(UnsupportedEncodingException ex){
				return null;
			}catch(IllegalArgumentException ex){
				Logs.geterrorLogger().error("url Decoder error : " + url,ex);
				return null;
			}
		}
		catch (UnsupportedEncodingException ex) {
			return null;
		}  	
    }
    
    public static String UrlDecoderGBK(String url){
    	try {
    		if(url!=null){
    			return URLDecoder.decode(url,"GBK");
    		}else{
    			return null;
    		}
    		
		} catch (UnsupportedEncodingException ex) {
			//com.wantuan.util.LogUtil.dealWith(ex,StringUtil.class);
			return null;
		}  	
    }
    
    public static String UrlEncoder(String url){
    	try {
    		if(url==null) url = "";
			String tmp= URLEncoder.encode(url,"UTF-8");
			return tmp;
		} catch (UnsupportedEncodingException ex) {
			//com.wantuan.util.LogUtil.dealWith(ex,StringUtil.class);
			return null;
		}  	
    }
    

    /**
     * 字符串替换，将 source 中的 oldString 全部换成 newString
     *
     * @param source 源字符串
     * @param oldString 老的字符串
     * @param newString 新的字符串
     * @return 替换后的字符串
     */
    public static String replaceStr(String source, String oldString, String newString) {
        StringBuffer output = new StringBuffer();

        int lengthOfSource = source.length();   // 源字符串长度
        int lengthOfOld = oldString.length();   // 老字符串长度

        int posStart = 0;   // 开始搜索位置
        int pos;            // 搜索到老字符串的位置
        
        String lower_s=source.toLowerCase();		//不区分大小写
        String lower_o=oldString.toLowerCase();
        
        while ((pos = lower_s.indexOf(lower_o, posStart)) >= 0) {
            output.append(source.substring(posStart, pos));

            output.append(newString);
            posStart = pos + lengthOfOld;
        }

        if (posStart < lengthOfSource) {
            output.append(source.substring(posStart));
        }

        return output.toString();
    }


	/**
	 * 2007-7-30 added by lxs
	 * 将原有的字符串按照需要的长度显示,超出的长度用..代替。
	 * 给定的长度应包含2位..的长度数。
	 *0x3400->13312->'?' 0x9FFF->40959->? 0xF900->63744->?
	 */
	public static String getPointStr(String str,int length){
		if(str==null || "".equals(str)){
			return "";
		}
		
		if(length<=0){
			return str;
		}
		if(getStrLength(str)>length){
//			System.out.println("string.length = " + getStrLength(str));
//			str=getLeftStr(str,length-2);
			str = getOutputPintString(str, length);
		}
		return str;
	}

	public static String getLeftStr(String str,int length){
		if(str == null || "".equals(str)){
			return "";
		}
		int index = 0;
		int strLength = str.length();
//		if (length>strLength) 
//			length = strLength;
		if (length<0)
			length=0;
	
//		System.out.println("getLeftStr length = " + length);
		char[] charArray = str.toCharArray();
		//希腊字母表使用从0x0370到0x03FF的代码，斯拉夫语使用从0x0400到0x04FF的代码，美国使用从0x0530到0x058F的代码，希伯来语使用从0x0590到0x05FF的代码。
		//中国、日本和韩国的象形文字（总称为CJK）占用了从0x3000到0x9FFF的代码。
		for(;index<length; index++){
			if (index >= strLength){
				break;
			}
			if(((charArray[index]>=0x3000)&&(charArray[index]<0x9FFF))||(charArray[index]>=0xF900)){
				length --;
//				System.out.println("length = " + length);
			}
			if (charArray[index]=='&'){
				//&lt;
				if (strLength>(index+3) && charArray[index+1]=='l' && charArray[index+2]=='t' && charArray[index+3]==';'){
					length += 3 ;
					index +=3;
				}
				//&#46;
				if(strLength>(index+4) && charArray[index+1]=='#' && charArray[index+2]=='4' && charArray[index+3]=='6' && charArray[index+4]==';' ){
					length += 4 ;
					index += 4 ;
				}
				//&nbsp;
				if (strLength>(index+5) && charArray[index+1]=='n' && charArray[index+2]=='b' && charArray[index+3]=='s' && charArray[index+4]=='p' && charArray[index+5]==';'){
					length += 5 ;
					index += 5 ;
				}
				//&quot;
				if (strLength>(index+5) && charArray[index+1]=='q' && charArray[index+2]=='u' && charArray[index+3]=='o' && charArray[index+4]=='t' && charArray[index+5]==';'){
					length += 5 ;
					index += 5 ;
				}
				//&acute;
				if (strLength>(index+6) && charArray[index+1]=='a' && charArray[index+2]=='c' && charArray[index+3]=='u' && charArray[index+4]=='t' && charArray[index+5]=='e' && charArray[index+6]==';'){
					length += 6 ;
					index += 6 ;
				}
				//&cedil;
				if (strLength>(index+6) && charArray[index+1]=='c' && charArray[index+2]=='e' && charArray[index+3]=='d' && charArray[index+4]=='i' && charArray[index+5]=='l' && charArray[index+6]==';'){
					length += 6 ;
					index += 6 ;
				}
//				if (length>strLength) 
//					length = strLength;
			}
		}
//		System.out.println("index = " + index);
		String returnStr = str.substring(0, index);

		returnStr += "..";
		return returnStr;
	}
	public static boolean isCn(String strInput){
		if(isEmpty(strInput)){
			return false;
		}
		String reg = "[\\u4e00-\\u9fa5]+";
		return strInput.matches(reg);
	}
	public static boolean isChinese(String str){
		if(str==null || "".equals(str)){
			return true;
		}
		char[] charArray = str.toCharArray();
				
		for(int i = 0; i < charArray.length; i++){
			if(((charArray[i]>=0x3400)&&(charArray[i]<0x9FFF))||(charArray[i]>=0xF900)){
				return false;
			}
		}
		return true;
	}

	public static int getStrLength(String str){
		if(str==null || "".equals(str)){
			return 0;
		}
		char[] charArray = str.toCharArray();
		int length = 0;
		int strLength = str.length();
		
		for(int i = 0; i < charArray.length; i++){
			if(((charArray[i]>=0x3400)&&(charArray[i]<0x9FFF))||(charArray[i]>=0xF900)){
				length += 2;
			}else if(charArray[i]=='&'){
				//&lt;
				if (strLength>(i+3) && charArray[i+1]=='l' && charArray[i+2]=='t' && charArray[i+3]==';'){
					length ++;
					i+=3;
				}
				//&#46;
				if(strLength>(i+4) && charArray[i+1]=='#' && charArray[i+2]=='4' && charArray[i+3]=='6' && charArray[i+4]==';' ){
					length ++;
					i+=4;
				}
				//&nbsp;
				if (strLength>(i+5) && charArray[i+1]=='n' && charArray[i+2]=='b' && charArray[i+3]=='s' && charArray[i+4]=='p' && charArray[i+5]==';'){
					length ++;
					i+=5;
				}
				//&quot;
				if (strLength>(i+5) && charArray[i+1]=='q' && charArray[i+2]=='u' && charArray[i+3]=='o' && charArray[i+4]=='t' && charArray[i+5]==';'){
					length ++;
					i+=5;
				}
				//&acute;
				if (strLength>(i+6) && charArray[i+1]=='a' && charArray[i+2]=='c' && charArray[i+3]=='u' && charArray[i+4]=='t' && charArray[i+5]=='e' && charArray[i+6]==';'){
					length ++;
				}
				//&cedil;
				if (strLength>(i+6) && charArray[i+1]=='c' && charArray[i+2]=='e' && charArray[i+3]=='d' && charArray[i+4]=='i' && charArray[i+5]=='l' && charArray[i+6]==';'){
					length ++;
					i+=6;
				}
			}else {
				length ++;
			}
		}
		return length;
	}

	public static int getStrLengthRank(String str){
		if(str==null || "".equals(str)){
			return 0;
		}
		char[] charArray = str.toCharArray();
		int length = 0;
		int strLength = str.length();
		
		for(int i = 0; i < charArray.length; i++){
			if(((charArray[i]>=0x3400)&&(charArray[i]<0x9FFF))||(charArray[i]>=0xF900)){
				length += 2;
			}else if(charArray[i]=='&'){
				//&lt;
				if (strLength>(i+3) && charArray[i+1]=='l' && charArray[i+2]=='t' && charArray[i+3]==';'){
					length ++;
					i+=3;
				}
				//&#46;
				if(strLength>(i+4) && charArray[i+1]=='#' && charArray[i+2]=='4' && charArray[i+3]=='6' && charArray[i+4]==';' ){
					length ++;
					i+=4;
				}
				//&nbsp;
				if (strLength>(i+5) && charArray[i+1]=='n' && charArray[i+2]=='b' && charArray[i+3]=='s' && charArray[i+4]=='p' && charArray[i+5]==';'){
					length ++;
					i+=5;
				}
				//&quot;
				if (strLength>(i+5) && charArray[i+1]=='q' && charArray[i+2]=='u' && charArray[i+3]=='o' && charArray[i+4]=='t' && charArray[i+5]==';'){
					length ++;
					i+=5;
				}
				//&acute;
				if (strLength>(i+6) && charArray[i+1]=='a' && charArray[i+2]=='c' && charArray[i+3]=='u' && charArray[i+4]=='t' && charArray[i+5]=='e' && charArray[i+6]==';'){
					length ++;
				}
				//&cedil;
				if (strLength>(i+6) && charArray[i+1]=='c' && charArray[i+2]=='e' && charArray[i+3]=='d' && charArray[i+4]=='i' && charArray[i+5]=='l' && charArray[i+6]==';'){
					length ++;
					i+=6;
				}
			}else {
				if((strLength >(i+1) && charArray[i]!=' ' && charArray[i+1] == ' ')||(i+1) == strLength){
					length ++;
				}
			}
		}
		return length;
	}
	
	
	


    /**
     * 将字符串格式化成 HTML 代码输出
     * 只转换特殊字符，适合于 HTML 中的表单区域
     *
     * @param str 要格式化的字符串
     * @return 过滤后的字符串
     */
    public static String inputToHtml(String str){     
	    if (StringUtil.isEmpty(str)){
	    	return str;
	    }
	    str = replaceStr(str, "<", "&lt;");
	    str = replaceStr(str, ">", "&gt;");
	    str = replaceStr(str, "/", "&#47;");
	    str = replaceStr(str, "\\", "&#92;");
	    str = replaceStr(str, "\r\n", "<br/>");
	    str = replaceStr(str, " ","&nbsp;"); 
	    str = replaceStr(str,"'","&acute;");
	    str = replaceStr(str,"\"","&quot;");
	    str = replaceStr(str,"(","&#40;");
	    str = replaceStr(str,")","&#41;");
	    //str = replaceStr(str,".","&#46;");
	   	return str;
   }
    
    /**
     * 将字符串格式化后 HTML 代码转成原始字符输出
     * 只有html编辑器中修改或输入表单中的修改需要调此函数
     *
     * @param str 格式化后的字符串
     * @return 原始的字符串
     */
    
    private static String outputToOrgStr(String str) {
	    if (StringUtil.isEmpty(str))    return null;
	    str = replaceStr(str, "&lt;","<");
	    str = replaceStr(str, "&gt;", ">");	
	    str = replaceStr(str, "&#47;","/");
	    str = replaceStr(str, "&#92;","\\");
	    str = replaceStr(str, "<br/>","\r\n");
	    str = replaceStr(str, "&nbsp;"," "); 
	    str = replaceStr(str, "&acute;","'");
	    str = replaceStr(str, "&quot;","\"");
	    //str = replaceStr(str, "&#46;",".");
	    str = replaceStr(str, "&#40;", "(");
	    str = replaceStr(str, "&#41;", ")");
	    
//	    str = replaceStr(str, "<br>","\r\n");
//	    str = replaceStr(str, "&cedil;",".");
//	    str = replaceStr(str, "    ","\t" );
	    return str;
    }
    
    /**
     * 将原始字符串中有危险的字符过滤
     * 用过html编辑器内容显示到网页上，需要保留大部分html标签
     *
     * @param str 格式化后的字符串
     * @return 过滤了危险之后的原始字符串
     */
    
    public static String outputToHtml(String str){
    	if (StringUtil.isEmpty(str))    return null;
    	
    	String[] html = {"onwaiting","onvolumechange","ontimeupdate","onsuspend","onstalled","onseeking","onseeked","onratechange","onprogress","onplaying","onplay","onpause","onloadstart","onloadedmetadata","onloadeddata","onended","onemptied","ondurationchange","oncanplaythrough","oncanplay","oninvalid","oninput","onforminput","onformchange","onundo","onstorage","onredo","onpopstate","onpageshow","onpagehide","ononline","onoffline","onmessage","onbeforeonload","onhaschange","onabort", "onactivate", "onafterprint", "onafterupdate", "onbeforeactivate", "onbeforecopy", "onbeforecut", "onbeforedeactivate", "onbeforeeditfocus", "onbeforepaste", "onbeforeprint", "onbeforeunload", "onbeforeupdate", "onblur", "onbounce", "oncellchange", "onchange", "onclick", "oncontextmenu", "oncontrolselect", "oncopy", "oncut", "ondataavailable", "ondatasetchanged", "ondatasetcomplete", "ondblclick", "ondeactivate", "ondrag", "ondragend", "ondragenter", "ondragleave", "ondragover", "ondragstart", "ondrop", "onerror", "onerrorupdate", "onfilterchange", "onfinish", "onfocus", "onfocusin", "onfocusout", "onhelp", "onkeydown", "onkeypress", "onkeyup", "onlayoutcomplete", "onload", "onlosecapture", "onmousedown", "onmouseenter", "onmouseleave", "onmousemove", "onmouseout", "onmouseover", "onmouseup", "onmousewheel", "onmove", "onmoveend", "onmovestart", "onpaste", "onpropertychange", "onreadystatechange", "onreset", "onresize", "onresizeend", "onresizestart", "onrowenter", "onrowexit", "onrowsdelete", "onrowsinserted", "onscroll", "onselect", "onselectionchange", "onselectstart", "onstart", "onstop", "onsubmit", "onunload","javascript", "vbscript", "expression", "applet", "meta", "xml", "blink", "link", "style", "script", "embed", "object", "iframe", "frame", "frameset", "ilayer", "layer", "bgsound", "title", "base"};
    	str=outputToOrgStr(str);
    	str = replaceStr(str, "<iframe","");
    	str = replaceStr(str, "</iframe","");
    	str = replaceStr(str, "<script","");
    	str = replaceStr(str, "</script","");
    	str = replaceStr(str, "<embed","");
    	str = replaceStr(str, "</embed","");
    	str = replaceStr(str, "<link","");
    	str = replaceStr(str, "</link","");
    	str = replaceStr(str, "script","");
    	str = replaceStr(str, "document","");
    	str = replaceStr(str, "createElement","");
    	str = replaceStr(str, "onerror","");
    	str = replaceStr(str, "body","");
    	str = replaceStr(str, "appendChild","");
    	str = replaceStr(str, "innerHTML","");
    	
    	for(int i=0;i<html.length;i++){
    		str = replaceStr(str, html[i],"");
    	}
    	
	    return str;
    }
    
	/**
	 * 过滤html输出与网站中可能产生冲突的标签
	 * @param str
	 * @return
	 */
	public static String filterWebsiteTag(String str){
		if(StringUtil.isEmpty(str)){
			return null;
		}
		str = fiterHtmlTag(str, "dd");
		str = fiterHtmlTag(str, "DD");
		str = fiterHtmlTag(str, "dl");
		str = fiterHtmlTag(str, "DL");
		str = fiterHtmlTag(str, "dt");
		str = fiterHtmlTag(str, "DT");
		return str;
	}

	public static String Html2Text(String inputString) {   
		try {
			String htmlStr = inputString; //含html标签的字符串   
			String textStr ="";   
			java.util.regex.Pattern p_script;   
			java.util.regex.Matcher m_script;   
			java.util.regex.Pattern p_style;   
			java.util.regex.Matcher m_style;   
			java.util.regex.Pattern p_html;   
			java.util.regex.Matcher m_html;   
			     
			   
		    String regEx_script = "<[\\s]*?script[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?script[\\s]*?>"; //定义script的正则表达式{或<script[^>]*?>[\\s\\S]*?<\\/script> }   
		    String regEx_style = "<[\\s]*?style[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?style[\\s]*?>"; //定义style的正则表达式{或<style[^>]*?>[\\s\\S]*?<\\/style> }   
			String regEx_html = "<[^>]+>"; //定义HTML标签的正则表达式   
			
			p_script = Pattern.compile(regEx_script,Pattern.CASE_INSENSITIVE);   
			m_script = p_script.matcher(htmlStr);   
			htmlStr = m_script.replaceAll(""); //过滤script标签   
			
			p_style = Pattern.compile(regEx_style,Pattern.CASE_INSENSITIVE);   
			m_style = p_style.matcher(htmlStr);   
			htmlStr = m_style.replaceAll(""); //过滤style标签   
			
			p_html = Pattern.compile(regEx_html,Pattern.CASE_INSENSITIVE);   
			m_html = p_html.matcher(htmlStr);   
			htmlStr = m_html.replaceAll(""); //过滤html标签   
		        
		    textStr = htmlStr;   
		    textStr = replaceStr(textStr, "\r\n", "");
		    textStr = replaceStr(textStr, "	", "");
	        
  
	     
      	  	return textStr;//返回文本字符串   
	      }catch(Exception e) {   
		         Logs.geterrorLogger().error("Html2Text: ", e);  
		         return null;
		  } 
      }     	
	
	
	/**
	 * 过滤指定标签
	 * @param str
	 * @param tag
	 * @return
	 */
	public static String fiterHtmlTag(String str, String tag) {
		   String regxp = "<\\s*" + tag + "\\s*([^>]*)\\s*>";
		   Pattern pattern = Pattern.compile(regxp);
		   Matcher matcher = pattern.matcher(str);
		   StringBuffer sb = new StringBuffer();
		   boolean result1 = matcher.find();
		   while (result1) {
		    matcher.appendReplacement(sb, "");
		    result1 = matcher.find();
		   }
		   matcher.appendTail(sb);
		   str = sb.toString();
		   regxp = "</" + tag + ">";
		   pattern = Pattern.compile(regxp);
		   matcher = pattern.matcher(str);
		   sb = new StringBuffer();
		   result1 = matcher.find();
		   while (result1) {
		    matcher.appendReplacement(sb, "");
		    result1 = matcher.find();
		   }
		   matcher.appendTail(sb);
		   return sb.toString();
	}
	
    
    /**
	 * 过滤html标签
	 * @param htmlStr
	 * @param length
	 * @return
	 */
	 public static String htmlToStr(String htmlStr,int length) {
		if (htmlStr == null) {
			return "";
		}
		StringBuffer result = new StringBuffer();
		int count = 0;
		
		boolean flag = true;
		htmlStr = htmlStr.replace("&lt;", "<");
		char[] a = htmlStr.toCharArray();
		for (int i = 0; i < a.length; i++) {
			if (a[i] == '<') {
				flag = false;
				continue;
			}
			if (a[i] == '>') {
				flag = true;
				continue;
			}
			if (flag == true) {
				count++;
			}
			if(count > length){
				result.append("...");
				break;
			}
			if(flag == true){
				result.append(a[i]);
			}
		}
		return result.toString();
	} 
    
    //  全角转半角
	public static String SBCchange(String str){
		String outStr = "";
		byte[] b = null;
		try{
			for(int i=0;i<str.length();i++){
				b = str.substring(i,i+1).getBytes("unicode");
				if(b[3]==-1){
					b[2] = (byte)(b[2]+32);
					b[3] = 0;
					outStr += new String(b,"unicode");
				}else{
					outStr += str.substring(i,i+1);
				}
			}
		}catch(Exception e){
			Logs.geterrorLogger().error(e.getMessage(),e); 
		}
		return outStr;
	}
	
	//半角转全角
	public static String BQchange(String str){
		String outStr = "";
		byte[] b = null;
		try{
			for(int i=0;i<str.length();i++){
				b = str.substring(i,i+1).getBytes("unicode");
				if(b[3]!=-1){
					b[2] = (byte)(b[2]-32);
					b[3] = -1;
					outStr += new String(b,"unicode");
				}else{
					outStr += str.substring(i,i+1);
				}
			}
		}catch(Exception e){
			Logs.geterrorLogger().error(e.getMessage(),e); 
		}
		return outStr;
	}
    
	public static long getIpValue(String ip){
		try{
			String[] str=splitStr(ip,'.');
			return Long.valueOf(str[0])*256*256*256+Long.valueOf(str[1])*256*256+Long.valueOf(str[2])*256+Long.valueOf(str[3]);
		}catch(Exception ex){
			return 0;
		}
	}
	

	
	public static int rand(int begin,int end){
		int num=Random();
		while(num<begin||num>end){
			num=Random();		
		}
		return num;
	}
	private static long next=1;
	public static int Random(){
		long x ;
		double i ;
		double fina ;
		long max=1;
		x = 4294967295l;
		x += 1 ;
		next *= ((long)134775813);
		System.out.println(next);
		next += 1 ;
		next = next % x ;
		i = ((double)next) / (double)4294967295l ;
		fina = (double) (max * i) ;
		int num=(int)(fina*10);
		return num;
	}
	

	public static String  changeIP(String ip){
		if(isEmpty(ip)){
			return "";
		}
		String ip_t=ip;
		int t=ip.lastIndexOf(".");
		if(t>0){
			ip_t=ip.substring(0, t+1);
			ip_t+="*";
		}
		return ip_t;
	}

 
	 
	public static String getStringMutiEncode(String s) throws UnsupportedEncodingException{
		String s1 = new String(s);
		String s2 = new String(s.getBytes("GBK"),"UTF8");
		String s3 = new String(s.getBytes("UTF8"),"GBK");
		System.out.println("s1 = "+s1);
		System.out.println("s2 = "+s2);
		System.out.println("s3 = "+s3);
		
		int c1 = 0,c2 = 0,c3=0;
		
		c1 = getChineseLength(s1);
		c2 = getChineseLength(s2);
		c3 = getChineseLength(s3);
		
		System.out.println("c1 = " + c1);
		System.out.println("c2 = " + c2);
		System.out.println("c3 = " + c3);
		if (c2<c1&&c2>0){
			s1=s2;
			c1=c2;
			System.out.println("r2");
		}
		if (c3<c1&&c3>0){
			s1=s3;
			System.out.println("r3");
		}
		return s1;
	}
	
	public static int getChineseLength(String s){// encoding 0:UTF8  1:GBK
		int rv = 0;
		for (int i =0 ;i<s.length();i++){
			char c = s.charAt(i);
			if (c>=0x3000 && c<=0x9FFF){
				rv ++;
			}
		}
		System.out.println(rv);
		return rv;
	}
	
	public static String getemailByReplace(String s){
		String bb = s.replaceAll("[a-z|\\d|||_|-|@|.]", "*");
		return bb;
	}
	
	


	
	public static String filterCHNTOENG(String keyword){
		if(keyword==null) return "";
		keyword = StringUtil.replaceStr(keyword, "‘" , "'");
		keyword = StringUtil.replaceStr(keyword, "’" , "'");
		keyword = StringUtil.replaceStr(keyword, "“" , "\"");
		keyword = StringUtil.replaceStr(keyword, "”" , "\"");
		keyword = StringUtil.replaceStr(keyword, "。" , ".");
		keyword = StringUtil.replaceStr(keyword, "，" , ",");
		keyword = StringUtil.replaceStr(keyword, "！" , "!");
		keyword = StringUtil.replaceStr(keyword, "？" , "?");
		keyword = StringUtil.replaceStr(keyword, "：" , ":");
		keyword = StringUtil.replaceStr(keyword, "；" , ";");
		return keyword;
	}
	


	public static int wordLength(String str){
		if (str == null || str.length()==0)
			return 0;
		int flag = -1;
		 int sum=0;  
		 for(int i=0;i<str.length();i++)  
		 {  
			if ((str.charAt(i)>=0) && (str.charAt(i)<=255)){
				if(flag!=0){
					flag = 0;  
		 			sum=sum+1;
				}  
			}else{
				flag = 1;
				sum=sum+1;
			}  
		 }  
		return sum;  
	}
	
	static String[] weeks = { "日", "一", "二", "三", "四", "五", "六" };

	public static Integer outInt(Integer num){
		if(num==null)
			return 0;
		else{
			return num;
		}
	}
	public static String toIntegerSplitOut(Integer num){
        NumberFormat nf = NumberFormat.getInstance();
        //设置是否使用分组
        nf.setGroupingUsed(true);
        return nf.format(num);
	}
	
	public static String replTag(String tag) {
		StringBuffer sbuf = new StringBuffer();
		tag = tag.replaceAll(",", " ");
		String[] strArray = tag.split(" +");
		for(int i=0;i<strArray.length;i++){
			if(i!=0)sbuf.append(",");
			sbuf.append(strArray[i]);
		}
		return sbuf.toString();
	}
	

	
	public static String filter_word(String keyword){
		try{
			StringBuffer sbuf = new StringBuffer();
			char[] charArray = keyword.toCharArray();
			int length = charArray.length;
			for(int index = 0 ; index < length ; index++){
				if((charArray[index] >= 0x3000 && charArray[index] < 0x9FFF) || (charArray[index]>=0xF900) || (charArray[index] >= 32 && charArray[index] <= 126)){
					sbuf.append(charArray[index] + "");
				}
			}
			return sbuf.toString();
		}catch(Exception ex){
			return keyword;
		}
	}
	public static boolean isChinese(char c) {
		Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
		if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
			return true;
		}
		return false;
	}
	/** 过滤乱码 **/
	public static String isMessyCode(String strName) {
		Pattern p = Pattern.compile("\\s*|\t*|\r*|\n*,"); 
		Matcher m = p.matcher(strName);  
		String after = m.replaceAll("");
		//String temp=after.replaceAll("\\p{P}", "");
		char[] ch = after.trim().toCharArray();
		StringBuffer sbuf = new StringBuffer();
		for (int i = 0; i < ch.length; i++) {
			char c = ch[i];
			if(!Character.isLetterOrDigit(c)){  //确定指定字符是否为字母或数字
				if(!(c+"").equals(",")){
					if(isChinese(c)){
						sbuf.append(c + "");
					}
				}else{
					sbuf.append(c + "");
				}
			}else{
				sbuf.append(c + "");
			}
		}
		return sbuf.toString();
	}
	
	public static long ipToLong(String strIP){
		try{
			if(strIP == null || strIP.length() == 0){
				return 0L;
			}
			long[] ip = new long[4];  
			int position1 = strIP.indexOf(".");  
			int position2 = strIP.indexOf(".",position1+1);  
			int position3 = strIP.indexOf(".",position2+1);      
			ip[0] = Long.parseLong(strIP.substring(0,position1));  
			ip[1] = Long.parseLong(strIP.substring(position1+1,position2));  
			ip[2] = Long.parseLong(strIP.substring(position2+1,position3));  
			ip[3] = Long.parseLong(strIP.substring(position3+1));  
			return (ip[0]<<24)+(ip[1]<<16)+(ip[2]<<8)+ip[3]; 
		}catch(Exception ex){
			return 0L;
		}
	} 
	public static String longToip(long ipLong) {   
        long mask[] = {0x000000FF,0x0000FF00,0x00FF0000,0xFF000000};   
        long num = 0;   
        StringBuffer ipInfo = new StringBuffer();   
        for(int i=0;i<4;i++){   
            num = (ipLong & mask[i])>>(i*8);   
            if(i>0) ipInfo.insert(0,".");   
            ipInfo.insert(0,Long.toString(num,10));   
        }   
        return ipInfo.toString();   
    }
	

	public static String getSqlSubStatement(long[] ids){
		StringBuffer sbuf = new StringBuffer(); 
		if(ids == null || ids.length<=0){
			return sbuf.toString();
		}
		for (int i = 0; i < ids.length; i++) {
			if(i != 0)sbuf.append(",");
			sbuf.append(ids[i]);
		}
		return sbuf.toString();
	}
	
	public static String getSqlSubStatement(int[] ids){
		StringBuffer sbuf = new StringBuffer(); 
		if(ids == null || ids.length<=0){
			return sbuf.toString();
		}
		for (int i = 0; i < ids.length; i++) {
			if(i != 0)sbuf.append(",");
			sbuf.append(ids[i]);
		}
		return sbuf.toString();
	}
	
	public static String toRRShare(String html){
		if(html == null || html.trim().length() == 0){
			return "";
		}
		html = replaceStr(html, "\r\n", "<br>");
		return html;
	}
	
	public static int getArrangePassRate(int rate){
		int rateStyle = 0;
		if(rate >= 1 && rate <=10){
			rateStyle = 1;
		}else if(rate >= 11 && rate <=20){
			rateStyle = 2;
		}else if(rate >= 21 && rate <=30){
			rateStyle = 3;
		}else if(rate >= 31 && rate <=40){
			rateStyle = 4;
		}else if(rate >= 41 && rate <=50){
			rateStyle = 5;
		}else if(rate >= 51){
			rateStyle = 6;
		}
		return rateStyle;
	}
	
    public static String null2nothing(String sourceStr){
    	return null2Value(sourceStr, "");
    }
    
    public static String null2Value(String sourceStr, String defaultValue){
    	if(sourceStr == null) return defaultValue;
    	else return sourceStr;
    }
    

	

	 public static int[] getSequence(int no) {
         int[] sequence = new int[no];
         for(int i = 0; i < no; i++){
             sequence[i] = i;
          }
         Random random = new Random();
         for(int i = 0; i < no; i++){
             int p = random.nextInt(no);
             int tmp = sequence[i];
             sequence[i] = sequence[p];
             sequence[p] = tmp;
         }
          return sequence;
      }

	public static Set<Long> stringToSet(String ids){
		String[] idArr = ids.split("\\|");
		Set<Long> set = new HashSet<Long>();
		
		if(idArr != null && idArr.length > 0) {
			for (int i = 0; i < idArr.length; i++) {
				Long productId = StringUtil.toLong(idArr[i]);
				if(productId != null){
					set.add(productId);
				}
			}
		}
		return set;
	}
	
	public static String setToString(Set<Long> set){
		if(set == null || set.size() == 0) return null;
		StringBuilder builder = new StringBuilder();
		Iterator<Long> iter = set.iterator();
		int i = 0;
		while(iter.hasNext()){
			if(i != 0) builder.append(",");
			builder.append(iter.next());
			i++;
		}
		return builder.toString();
	}
	public static String[] setToStringArr(Set<Long> set){
		if(set == null || set.size() == 0) return null;
		int size = set.size() % 1000 == 0 ? set.size() / 1000 : set.size() / 1000 + 1;
		int i = 0;
		String [] result = new String[size];
		Iterator<Long> iter = set.iterator();
		Set<Long> tmp_set = new HashSet<Long>();
		while(iter.hasNext()){
			tmp_set.add(iter.next());
			if(tmp_set.size() >= 1000 || i*1000 + tmp_set.size() == set.size()){
				result[i] = setToString(tmp_set);
				tmp_set = new HashSet<Long>();
				i++;
			}
		}
		return result;
	}
	
	public static String listToString(List<Long> list){
		if(list == null || list.size() == 0) return null;
		
		StringBuilder builder = new StringBuilder();
		Iterator<Long> iter = list.iterator();
		int i = 0;
		while(iter.hasNext()){
			if(i != 0) builder.append(",");
			builder.append(iter.next());
			i++;
		}
		return builder.toString();
	}
	

	public static float getMoney(float money){
		float privilege = 0;
		if(money >= 100.00 && money < 200.00){
			privilege = 20;
		}else if(money >= 200.00 && money < 500.00){
			privilege = 40;
		}else if(money >= 500.00 && money < 1000.00){
			privilege = 200;
		}else if(money >= 1000.00 && money < 1500.00){
			privilege = 400;
		}else if(money >= 1500.00 && money < 2000.00){
			privilege = 600;
		}else if(money >= 2000.00 && money < 5000.00){
			privilege = 800;
		}else if(money >= 5000.00 && money < 10000.00){
			privilege = 2000;
		}else if(money >= 10000.00){
			privilege = 4000;
		}
		money = money - privilege;
		return money;
	}
	

	/**
	 * 过滤十进制，16进制汉字
	 * @param str
	 * @return
	 */
	public static boolean filterCode(String str){
		boolean flag = false;
		if(str == null || str.trim().length() == 0){
			return flag;
		}
		String type = "&(amp;)?#\\w{5};";
		Pattern pattern = Pattern.compile(type);
		Matcher matcher = pattern.matcher(str);
		flag = matcher.matches();
		return flag;
	}
	public static String filterString(String str){
		str = str.toLowerCase().trim();
		str = str.replaceAll("&nbsp;|<br>|<BR>|[\\s]|[\u3000]", "");
		return str;
	}
	

	

	
	/**
	 * 十进制转换成二进制
	 * @param ten
	 * @return
	 */
	public static String tenToEr(long ten){
		String s = "";
		while(ten > 0){
			long i = ten % 2;
			s = i + s;
			ten = ten/2;
		}
		int t = s.length();
		for (int i = t; i < 32; i++) {
			s = "0"+s;
		}
		return s;
	}
	
	/**
	 * 根据ip获取  子网掩码（起始ip 和 结束ip长度必须一致）
	 * @param startIP 起始ip(必须是二进制)
	 * @param endIP 结束ip (必须是二进制)
	 * @return
	 */
	public static int getIPMark(String startIP,String endIP){
		int result = 0;
		if(startIP == null|| startIP.trim().length() == 0 || endIP == null || endIP.trim().length() == 0){
			return result;
		}
		for (int i = 0; i < startIP.length(); i++) {
			char start = startIP.charAt(i);
			char end = endIP.charAt(i);
			if(start != end){
				result = i;
				break;
			}
		}
		return result;
	}
	
	/**
	 * 根据ip获取  子网掩码
	 * @param startIP 
	 * @param endIP
	 * @return
	 */
	public static int getIPMark(long startIP,long endIP){
		int result = 0;
		if(startIP <= 0 || endIP <= 0){
			return result;
		}
		result = getIPMark(tenToEr(startIP), tenToEr(endIP));
		return result;
	}
	
	/**
     * ip校验
     * @param s
     * @return 格式是否正确
     */
    public static Boolean isIpAddress(String s){
         String regex = "(((2[0-4]\\d)|(25[0-5]))|(1\\d{2})|([1-9]\\d)|(\\d))[.](((2[0-4]\\d)|(25[0-5]))|(1\\d{2})|([1-9]\\d)|(\\d))[.](((2[0-4]\\d)|(25[0-5]))|(1\\d{2})|([1-9]\\d)|(\\d))[.](((2[0-4]\\d)|(25[0-5]))|(1\\d{2})|([1-9]\\d)|(\\d))";
         Pattern p = Pattern.compile(regex);
         Matcher m = p.matcher(s);
         return m.matches();
    }
    /**
     * email校验
     * @param email
     * @return  格式是否正确
     */
    public static Boolean isEmail(String email){
    	if(isEmpty(email)){
    		return false;
    	}
		String regex = "^([a-zA-Z0-9_\\-\\.\\+]+)@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\\]?)$";
		Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(email);
        return m.matches();
	}
    /**
     * password校验
     * @param password
     * @return  格式是否正确
     */
    public static Boolean isPassword(String password){
    	if(isEmpty(password)){
    		return false;
    	}
		String regex = "^[A-Za-z0-9_-]+$";
		Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(password);
        return m.matches();
	}
   /**
    * 手机号校验
     * @param mobiles
     * @return  格式是否正确
     * 
     * "^((176)|(170)|(13[0-9])|(14[0-9])|(15[^4,\\D])|(18[0-9]))\\d{8}$"   增加(170)号码验证  dqx---- 2014年7月3日13:43:19 增加(177,178)号码验证  lz---- 2014年10月10日  增加验证国际号码(以+开头) yyh 2014/10/30
    */
    public static boolean isMobile(String mobiles){
    	if(isEmpty(mobiles)){
    		return false;
    	}
    	String regex1 = "^((176)|(177)|(178)|(170)|(173)|(13[0-9])|(14[0-9])|(15[^4,\\D])|(18[0-9]))\\d{8}$";
    	String regex2 = "^\\+[0-9]{1,4}?\\d{6,11}$";
		Pattern p1 = Pattern.compile(regex1);
		Pattern p2 = Pattern.compile(regex2);
		Matcher m1 = p1.matcher(mobiles);
		Matcher m2 = p2.matcher(mobiles);
		return m1.matches()||m2.matches();
	}
    public static boolean isChineseMobile(String mobiles){
    	if(isEmpty(mobiles)){
    		return false;
    	}
    	String regex1 = "^(\\+86)?((176)|(177)|(178)|(170)|(173)|(13[0-9])|(14[0-9])|(15[^4,\\D])|(18[0-9]))\\d{8}$";
		Pattern p1 = Pattern.compile(regex1);
		Matcher m1 = p1.matcher(mobiles);
		return m1.matches();
	}
	public static long[] getLong(String str){
		if(str == null || str.trim().length()==0||"".equals(str)) return null;
		String[] strs = str.split(",");
		long[] ids = null;
		if(strs != null && strs.length>0){
			ids = new long[strs.length];
			for (int i = 0 ;i < strs.length; i++) {
				ids[i] = StringUtil.toLong(strs[i], 0L);
			}
		}
		return ids;
	}
	public static long[] getLong(String str,String charStr){
		if(str == null || str.trim().length()==0||"".equals(str)) return null;
		String[] strs = str.split(charStr);
		long[] ids = null;
		if(strs != null && strs.length>0){
			ids = new long[strs.length];
			for (int i = 0 ;i < strs.length; i++) {
				ids[i] = StringUtil.toLong(strs[i], 0L);
			}
		}
		return ids;
	}
	
	public static int[] getInteger(String str,String charStr){
		if(str == null || str.trim().length()==0||"".equals(str)) return null;
		String[] strs = str.split(charStr);
		int[] ids = null;
		if(strs != null && strs.length>0){
			ids = new int[strs.length];
			for (int i = 0 ;i < strs.length; i++) {
				ids[i] = StringUtil.toInteger(strs[i], 0);
			}
		}
		return ids;
	}
	
	/**
	 * 
	 * @param str
	 * @param len
	 * @return
	 */
	public static String toHtmlSubString(String str, int len) {   
        int index = len - 1; //下标比总数少一个   
        if (null == str || "".equals(str.trim())){   
            return "";   
        }   
        if (index <= 0){   
            return str;   
        }   
        byte[] bt = null;   
        try {   
        	bt = str.getBytes();   
        }catch (Exception e){   
            e.getMessage();   
        }   
        if (null == bt || bt.length <= len){   
           return str;   
        }   
        if (index > bt.length - 1){   
            index = bt.length - 1; //防越界   
        }   
        String substrx = null;
        //如果当前字节小于0，说明当前截取位置 有可能 将中文字符截断了   
        if (bt[index] < 0){   
            int jsq = 0;   
            int num = index;   
            while (num >= 0){   
                if (bt[num] < 0){   
                    jsq += 1; //计数   
                }else {   
                    break; //循环出口   
                }   
                num -= 1;   
            }   
 
            int m = 0;   
               //Unicode编码   
            	m = jsq % 2;   
                index -= m;   
                //这里是重点,去掉半个汉字(有可能是半个), m为0表示无一半汉字,   
                substrx = new String(bt, 0, index + 1)+ ".."; //当前被截断的中文字符整个不取   
                return substrx;   
        }else {   
            substrx = null;   
                //Unicode编码   
            substrx = new String(bt, 0, index + 1) + "..";   
            return substrx;   
        } 

    } 
	
	public static String getOutputPintString(String str,int len){
		try {
			if(str == null || str.trim().length() == 0){
				return "";
			}
			str = str.replaceAll("&nbsp;", " ");
			byte [] b = str.trim().getBytes();
			if(b.length <= len){
				return str;
			}
			StringBuffer sb = new StringBuffer();
			double sub = 0;
			for (int i = 0; i < str.length(); i++) {
				if(sub >= len-2){
					sb.append("..");
					break;
				}
				String tmp =  str.substring(i, i+1);
				sub += tmp.getBytes().length > 2 ? 2 : tmp.getBytes().length;
				char c = str.charAt(i);
				if(c >=65 && c <= 90){
					sub += 0.5;
				}
				sb.append(tmp);
			}
			return sb.toString();
		} catch (Exception e) {
			return str;
		}
		
	}

    /**
     * 2014年8月25日 对首页面文字截取的优化(每个汉字2字节英文1字节不区分大小写)
     * @param source
     * @param length
     * @return
     */
    public static String getOutputPintStringNew(String source, int length) {
        source = source.replaceAll("&nbsp;", " ");
        int textLength = source.length();
        int byteLength = 0;
        StringBuffer returnStr =  new StringBuffer();
        for(int i = 0; i<textLength && byteLength < length*2; i++){
            String str_i = source.substring(i, i+1);
            if(str_i.getBytes().length == 1){//英文
                byteLength++;
            }else{//中文
                byteLength += 2 ;
            }
            returnStr.append(str_i);
        }
        try {
            if(byteLength<source.getBytes("GBK").length){
                returnStr.append("..");
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return returnStr.toString();
    }

	public static String subStringIE(String str,int len){
		if(str == null || str.trim().length() == 0){
			return "";
		}
		byte [] b = str.trim().getBytes();
		if(b.length <= len){
			return str;
		}
		StringBuffer sb = new StringBuffer();
		double sub = 0;
		for (int i = 0; i < str.length(); i++) {
			String tmp = str.substring(i, i+1);
			sub +=getStringIELength(tmp);
			if(sub > len-2){
				sb.append("..");
				break;
			}
			sb.append(tmp);
		}
		return sb.toString();
	}
	
	public static double getStringIELength(String s){
		if(s == null || s.length() == 0){
			return 0;
		}
		byte [] b = s.getBytes();
		if(b.length > 1){
			return b.length;
		}else if("il: ".indexOf(s) > -1){
			return 0.4;
		}else if("fjt".indexOf(s) > -1){
			return 0.5;
		}else if("rI".indexOf(s) > -1){
			return 0.6;
		}else if("sJ-z".indexOf(s) > -1){
			return 0.7;
		}else if("aceghnuvxy".indexOf(s) > -1){
			return 0.9;
		}else if("EFLPSTZ".indexOf(s) > -1){
			return 1.1;
		}else if("BR".indexOf(s) > -1){
			return 1.2;
		}else if("ACDGFHmwUVXY".indexOf(s) > -1){
			return 1.3;
		}else if("KNOQ".indexOf(s) > -1){
			return 1.5;
		}else if("MW".indexOf(s) > -1){
			return 1.65;
		}else{
			return 1;
		}
		
	}



	 public static boolean isEnglish(String str){
		 String reg = "[a-zA-Z]";
		 int lng = str.length();
		 for (int i = 0; i < lng; i++) {
			String tmp = str.charAt(i) + "";
			if(tmp.matches(reg)){
				return true;
			}
		}
		 return false;
	 }
	 /**
	  * 是否包含非法字符
	  * \\/:*?\" ><|
	  * @param str
	  * @return true:包含非法字符 false 不包含非法字符
	  */
	 public static boolean isContainIllegalChar(String str){
		 if(str.indexOf("\\") == -1 && str.indexOf("/") == -1 &&  str.indexOf(":") == -1 &&  str.indexOf("*") == -1 &&  str.indexOf("?") == -1 &&  str.indexOf("\"") == -1 &&  str.indexOf(">") == -1 &&  str.indexOf("<") == -1 &&  str.indexOf("|") == -1){
			 return false;
	      }else{
	    	 return true;
	      }
		 
	 }

	/**
	 * 把str字符串最后一个字符 若为中文标点则转为英文标点
	 * @param str
	 * @return
	 */
	public static String lastCharacterCn2En(String str){
		if(str != null && str.length() > 0){
			String lastStr = str.substring(str.length()-1,str.length());
			String regex = "[，。？：；‘’！“”—……、（）【】{}《》－]";
			Pattern p=null; //正则表达式
	        Matcher m=null; //操作的字符串
	        p = Pattern.compile(regex);
	    	m = p.matcher(lastStr);
	    	if(m.matches()){
	    		str = str +" ";
	    	}
		}
		return str;
	}
	
	public static int getCharCount(String str,char c){
		int j = 0;
		for (int i = 0;i<str.length();i++){
			if (str.charAt(i)==c)
				j++;
		}
		return j;
	}

	public static String getWeekStr(int week){
		String weekStr = "";
		switch(week){
			case 1:weekStr = "一";break;
			case 2:weekStr = "二";break;
			case 3:weekStr = "三";break;
			case 4:weekStr = "四";break;
			case 5:weekStr = "五";break;
			case 6:weekStr = "六";break;
			case 7:weekStr = "日";break;
			default:break;
		}
		return weekStr;
	}

	public static String doubleDecimalCny(double value){
		if(value == 0){
			return "0";
		}else{
			String result = value+"";
			if(value % 1.0 == 0){
				result =  (long)value+"";
			} 
			if(result.indexOf("E")>-1){
				NumberFormat formate  = NumberFormat.getNumberInstance();
				formate.setMaximumFractionDigits(8);
				formate.setMaximumIntegerDigits(8);
				result = formate.format(value);
			}
			int dotIndex = result.indexOf(".");
			if(dotIndex>-1){
				String part1 = result.substring(0, dotIndex);
				if(result.length()>part1.length()+3){
					result = part1+ result.substring(dotIndex, part1.length()+3);
				}
			    return result;
			}else{
				return result;
			}
		}
	}
	
	public static String doubleDecimalBtc(double value){
		if(value == 0){
			return "0";
		}else{
			double result = value;
			String resultStr  = result+"";
			if(!StringUtil.isEmpty(resultStr) && resultStr.indexOf(".")>-1){
				String [] resultStrs = resultStr.split("[.]");
				if(resultStrs!=null && resultStrs.length>1&&resultStrs[1].length()>6){
					result =  (double) (Math.floor(value*1000000)/1000000.0);
					resultStr = result+"";
				}
			}
			if(result % 1.0 == 0){
				resultStr =  (long)result+"";
			} 
			if(resultStr.indexOf("E")>-1){
				NumberFormat formate  = NumberFormat.getNumberInstance();
				formate.setMaximumFractionDigits(8);
				formate.setMaximumIntegerDigits(8);
				resultStr = formate.format(result);
			}
			return resultStr;
		}
	}

	public static String doubleDecimalAll(double value){
		if(value == 0){
			return "0";
		}else{
			double result = value;
			String resultStr  = result+"";
			if(!StringUtil.isEmpty(resultStr) && resultStr.indexOf(".")>-1){
				String [] resultStrs = resultStr.split("[.]");
				if(resultStrs!=null && resultStrs.length>1&&resultStrs[1].length()>6){
					result =  (double) (Math.floor(value*1000000)/1000000.0);
					resultStr = result+"";
				}
			}
			if(result % 1.0 == 0){
				resultStr =  (long)result+"";
			} 
			NumberFormat formate  = NumberFormat.getNumberInstance();
			formate.setMaximumFractionDigits(12);
			formate.setMaximumIntegerDigits(12);
			resultStr = formate.format(result);
			return resultStr;
		}
		
	}

    /**
     * 带千分位format
     * @param value
     * @param rate
     * @return
     */
	public static String doubleFormat(double value, int rate){
		StringBuffer format= new StringBuffer("#,###,##0.");
		if (rate > 0) {
			for (int i = 0; i < rate; i++) {
				format.append("0");
			}
		}
		if(format.toString().endsWith(".")){
			format=new StringBuffer("#,###,###");
		}
		DecimalFormat df=new DecimalFormat(format.toString());
		return df.format(value);
	}

    /**
     * 带千分位format,保留传入数据的精度
     * @param value
     * @return
     */
    public static String doubleFormat(double value){
        String valueStr = value + "";
        int rate = 0;
        if(!StringUtil.isEmpty(valueStr) && valueStr.indexOf(".") > -1) {
            String[] tempStr = valueStr.split("\\.");
            if(tempStr.length > 1){
                rate = tempStr[1].length();
            }
            if(valueStr.contains("E")){
            	int vrate = toInteger(valueStr.substring(valueStr.indexOf("E")+1))+3;
            	rate -= vrate;
            }
        }
        return doubleFormat(value, rate);
    }

    /**
     * 带千分位format,保留传入数据的精度
     * @param value
     * @return
     */
    public static String doubleFormat(String value){
        String valueStr = value ;
        int rate = 0;
        if(!StringUtil.isEmpty(valueStr) && valueStr.indexOf(".") > -1) {
            String[] tempStr = valueStr.split("\\.");
            if(tempStr.length > 1){
                rate = tempStr[1].length();
            }
        }
        return doubleFormat(StringUtil.toDouble(value),rate);
    }
    /**
     * format 非千分位
     * @param value
     * @param rate
     * @return
     */
    public static String doubleFormatCommon(double value, int rate){
        StringBuffer format= new StringBuffer("0.");
        if (rate >= 0) {
            for (int i = 0; i < rate; i++) {
                format.append("0");
            }
        }
        DecimalFormat df=new DecimalFormat(format.toString());
        return df.format(value);
    }
    
    /**
     * 
    * @Title: doubleFormatCommonCut
    * @Description: 带千分位format,保留传入数据的精度,去掉尾部O
    * @author Lei Sir 
    * @date 2015-1-20 下午5:29:22
    * @param @param value
    * @param @param rate
    * @param @return    设定文件
    * @throws
     */
    public static String doubleFormatCommonCut(double value, int rate){
    	StringBuffer format= new StringBuffer("0.");
    	if (rate >= 0) {
    		for (int i = 0; i < rate; i++) {
    			format.append("0");
    		}
    	}
    	DecimalFormat df=new DecimalFormat(format.toString());
    	return df.format(value).replaceAll("0+?$", "").replaceAll("[.]$", "");
    }

    /**
	 * 格式化比特币，原有方法会截取到小数点前8位，该方法到10位
	 * @param value
	 * @return
	 */
	public static String doubleDecimalBtc10(double value){
		if(value == 0){
			return "0";
		}else{
			double result = value;
			String resultStr  = result+"";
			if(!StringUtil.isEmpty(resultStr) && resultStr.indexOf(".")>-1){
				String [] resultStrs = resultStr.split("[.]");
				if(resultStrs!=null && resultStrs.length>1&&resultStrs[1].length()>6){
					result =  (double) (Math.floor(value*1000000)/1000000.0);
					resultStr = result+"";
				}
			}
			if(result % 1.0 == 0){
				resultStr =  (long)result+"";
			} 
			if(resultStr.indexOf("E")>-1){
				NumberFormat formate  = NumberFormat.getNumberInstance();
				formate.setMaximumFractionDigits(10);
				formate.setMaximumIntegerDigits(10);
				resultStr = formate.format(result);
			}
			return resultStr;
		}
	}
	
	/**
	 * 格式化数字，小数位后10位
	 * @param value
	 * @return
	 */
	public static String doubleDecimalBtcSuffix10(double value){
		if(value == 0){
			return "0";
		}else{
			double result = value;
			String resultStr  = result+"";
			if(!StringUtil.isEmpty(resultStr) && resultStr.indexOf(".")>-1){
				String [] resultStrs = resultStr.split("[.]");
				if(resultStrs!=null && resultStrs.length>1&&resultStrs[1].length()>6){
					result =  (double) (Math.floor(value*1000000000)/1000000000.0);
					resultStr = result+"";
				}
			}
			if(result % 1.0 == 0){
				resultStr =  (long)result+"";
			} 
			if(resultStr.indexOf("E")>-1){
				NumberFormat formate  = NumberFormat.getNumberInstance();
				formate.setMaximumFractionDigits(10);
				formate.setMaximumIntegerDigits(10);
				resultStr = formate.format(result);
			}
			return resultStr;
		}
	}
	
	public static String doubleDecimalBtc3(double value){
		if(value == 0){
			return "0";
		}else{
			double result = (double) (Math.floor(value*1000)/1000.0);
			if(result % 1.0 == 0){
			    return (long)result+"";
			}else{
				return result+"";
			}
		}
	}
	
	/**
	 * 检测是否有emoji字符
	 * @param source
	 * @return 一旦含有就抛出
	 */

	private static boolean isNotEmojiCharacter(char codePoint) {
		return (codePoint == 0x0) ||
				(codePoint == 0x9) ||
				(codePoint == 0xA) ||
				(codePoint == 0xD) ||
				((codePoint >= 0x20) && (codePoint <= 0xD7FF)) ||
				((codePoint >= 0xE000) && (codePoint <= 0xFFFD)) ||
				((codePoint >= 0x10000) && (codePoint <= 0x10FFFF));
	}

	/**
	 * 过滤emoji 或者 其他非文字类型的字符
	 * @param source
	 * @return
	 */
	public static String filterEmoji(String source) {
		if(StringUtil.isEmpty(source)){
			return source;
		}
		StringBuilder buf = new StringBuilder();
		int len = source.length();
		for (int i = 0; i < len; i++) {
			char codePoint = source.charAt(i);
			if (isNotEmojiCharacter(codePoint)) {
				buf.append(codePoint);
			}
		}
		return buf.toString();
	}
	
	/**
	 * 检查字符串是否合法。超过6个字符不能包含okcoin 不能包含非中文，英文，下划线，字符
	 * @param nickName
	 * @return false :不合法  true :合法
	 */
	public static boolean checkNikeName(String nickName){
		if(isEmpty(nickName)){
			return false;
		}
		if(nickName.indexOf("客服")!=-1 || nickName.toLowerCase().indexOf("coin") != -1 || nickName.toLowerCase().indexOf("ok") != -1 || nickName.indexOf("运营")!=-1 || nickName.indexOf("管理")!=-1 ||  nickName.indexOf("版主")!=-1 || nickName.indexOf("斑竹")!=-1 || nickName.indexOf("服务")!=-1 || nickName.indexOf("群主")!=-1 || nickName.indexOf("经理")!=-1 || nickName.indexOf("财务")!=-1){
			return false;
		}
		if(nickName.length() < 5){
			return true;
		}
		if(nickName.toLowerCase().indexOf("okcoin") != -1  ){
			return false;
		}
		String keyStr = "o,k,c,o,i,n,_,-,—";
		String[] keyArr = keyStr.split(",");
		List<String> a = Arrays.asList(keyArr);
		List<String> keyList = new ArrayList<String>(a);  
		String back;
		String lowerNickName = nickName.toLowerCase();
		for(int i=0;i<lowerNickName.length();i++){
			String ch = lowerNickName.charAt(i)+"";
			back = i<lowerNickName.length()-1?lowerNickName.charAt(i+1)+"":"";
			if(keyList.contains(ch) && keyList.contains(back)&&ch!=back){
				keyList.remove(ch);
				if(keyList.size() <= 5){
					return false;
				}
			}else{
				if(keyList.size()!= 9)
					keyList = new ArrayList<String>(a);  
			}
		}
		String regex = "^[a-zA-Z0-9_\u4e00-\u9fa5_\\pP\\p{Punct}]+$";
		Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(nickName);
        return m.matches();
	}
	/**
	 * 格式化人民币，原有方法会截取到小数点前8位，该方法到10位
	 * @param value
	 * @return
	 */
	public static String doubleDecimalCny10(double value){
		if(value == 0){
			return "0";
		}else{
			String result = value+"";
			if(value % 1.0 == 0){
				result =  (long)value+"";
			}
			if(result.indexOf("E")>-1){
				NumberFormat formate  = NumberFormat.getNumberInstance();
				formate.setMaximumFractionDigits(10);
				formate.setMaximumIntegerDigits(10);
				result = formate.format(value);
			}
			int dotIndex = result.indexOf(".");
			if(dotIndex>-1){
				String part1 = result.substring(0, dotIndex);
				if(result.length()>part1.length()+3){
					result = part1+ result.substring(dotIndex, part1.length()+3);
				}
			    return result;
			}else{
				return result;
			}
		}
	}
	//截取8位
	public static String doubleDecimal8(double value){
		if(value == 0){
			return "0";
		}else{
			String result = value+"";
			if(value % 1.0 == 0){
				result =  (long)value+"";
			}
			if(result.indexOf("E")>-1){
				NumberFormat formate  = NumberFormat.getNumberInstance();
				formate.setMaximumFractionDigits(8);
				formate.setMaximumIntegerDigits(8);
				result = formate.format(value);
			}
			int dotIndex = result.indexOf(".");
			if(dotIndex>-1){
				String part1 = result.substring(0, dotIndex);
				result = part1+ result.substring(dotIndex,result.length()-dotIndex<10?result.length():10);
			    return result;
			}else{
				return result;
			}
		}
	}

	/**
	 * 随机大小写字符串
	 * @param length 字符串长度
	 * @return
	 */
	public static String randomStr(int length){
		StringBuffer str = new StringBuffer();
		for (int i = 0; i < length; i++) {
			Random ran = new Random();
			int choice = ran.nextInt(2)%2 == 0 ? 65 : 97;  //先通过 一个0-1的随机数 来选择是大写还是小写
			char x = (char)(choice + ran.nextInt(26)); 
			str.append(x);
		}
		return str.toString();
	}
	/**
	 * 随机大小写字母以及数字字符串
	 * @param length 字符串长度
	 * @return
	 */
	public static String randomStrAndNumber(int length){
		StringBuffer str = new StringBuffer();
		for (int i = 0; i < length; i++) {
			Random ran = new Random();
			int suiji = ran.nextInt(3)%3;
			int choice =  suiji== 2 ? 65 : suiji== 1?97:48;  //先通过 一个2-1-0的随机数 来选择是大写、小写还是数字
			char x = (char)(choice + (suiji==0?ran.nextInt(10):ran.nextInt(26))); 
			str.append(x);
		}
		return str.toString();
	}
	
	/**
	 * 去掉字符串中的 " [ ] 字符
	 * @param defstr
	 * @return
	 */
	public static String formatString(String defstr){
		if(isEmpty(defstr)){
			return "";
		}
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < defstr.length(); i++) {
			String s = defstr.substring(i,i+1);
			if(!s.endsWith("\"") && !s.endsWith("[") && !s.endsWith("]")){
				sb.append(s);
			}
		}
		return sb.toString();
	}
	
	public static String gbkToUtf(String content) throws UnsupportedEncodingException{
		if(isEmpty(content)){
			return "";
		}
		String iso = new String(content.getBytes("UTF-8"),"ISO-8859-1");
		String value = new String(iso.getBytes("ISO-8859-1"),"UTF-8");
		return value;
	}
	
	public static boolean isNum(String ss){
		if(isEmpty(ss)){
			return false;
		}
		for(int i=0;i<ss.length();i++){
			char a=ss.charAt(i);
			if(a<'0'||a>'9'){
				return false;
			}
		}
		return true;
	}
	
	public static String nickNmeStr(String nikeName){
		String nikeNamestr = nikeName;
		if(StringUtil.isNum(nikeName)){
			if(nikeNamestr.length() > 2){
				nikeNamestr = nikeName.substring(0,1)+"**"+nikeName.substring(nikeName.length()-1);
			}else{
				nikeNamestr = nikeName.substring(0,1)+"**";
			}
		}
		return nikeNamestr;
	}
	
	public static String replaceNickNme(String nikeName){
		if(StringUtil.isEmpty(nikeName)){
			return "***";
		}
		String nikeNamestr = nikeName;
		if(nikeNamestr.length() > 2){
			nikeNamestr = nikeName.substring(0,1)+"**"+nikeName.substring(nikeName.length()-1);
		}else{
			nikeNamestr = nikeName.substring(0,1)+"**";
		}
		return nikeNamestr;
	}
	public static long setBinaryIndex(long value,int bit,int index){
		long dex = 1l;
		if(getBinaryIndex(value, bit) == 1){
			if(index == 0){
				value = value - (dex<<bit-1);
			}
		}else{
			if(index == 1){
				value = value|(dex<<bit-1);
			}
		}
		return value;
	}
	public static int getBinaryIndex(long value,int bit){
		long remainder = 0;
		for(int i=0; i<bit;i++){
			long factor = value/2;
			remainder = value%2;
			if(factor == 0){
				if(i<bit-1){
					remainder = 0;
				}
				break;
			}
			value = factor;
		}
		return (int)remainder;
	}
	
	/**
	 * 
	 * 第1位:登录进行谷歌安全认证
	 * 2:邮箱绑定
	 * 3：是否短信验证
	 * 4:是否谷歌验证
	 * 5:免支付密码
	 * 6：免二次验证码
	 * 7:付款进行邮件验证
	 * @return
	 */
	public static int setBinaryIndex(int value,int bit,int index){
		if(getBinaryIndex(value, bit) == 1){
			if(index == 0){
				value = value - (1<<bit-1);
			}
		}else{
			if(index == 1){
				value = value|(1<<bit-1);
			}
		}
		return value;
	}
	/**
	 *第1位:登录进行谷歌安全认证
	 * 2:邮箱绑定
	 * 3：是否短信验证
	 * 4:是否谷歌验证
	 * 5:免支付密码
	 * 6：免二次验证码
	 * 7:付款进行邮件验证
	 * @return
	 */
	public static int getBinaryIndex(int value,int bit){
		int remainder = 0;
		for(int i=0; i<bit;i++){
			int factor = value/2;
			remainder = value%2;
			if(factor == 0){
				if(i<bit-1){
					remainder = 0;
				}
				break;
			}
			value = factor;
		}
		return remainder;
	}

    public static String ubb2html(String argString)
    {
        String tString = replaceStr(argString, "[&#47;", "[/");

        if (!tString.equals(""))
        {
            Boolean tState = true;
            tString = tString.replaceAll("\\[br\\]", "<br />");
            String[][] tRegexAry = {
                    {"\\[p\\]([^\\[]*?)\\[\\/p\\]", "$1<br />"},
                    {"\\[b\\]([^\\[]*?)\\[\\/b\\]", "<b>$1</b>"},
                    {"\\[i\\]([^\\[]*?)\\[\\/i\\]", "<i>$1</i>"},
                    {"\\[u\\]([^\\[]*?)\\[\\/u\\]", "<u>$1</u>"},
                    {"\\[ol\\]([^\\[]*?)\\[\\/ol\\]", "<ol>$1</ol>"},
                    {"\\[ul\\]([^\\[]*?)\\[\\/ul\\]", "<ul>$1</ul>"},
                    {"\\[li\\]([^\\[]*?)\\[\\/li\\]", "<li>$1</li>"},
                    {"\\[code\\]([^\\[]*?)\\[\\/code\\]", "<div class=\"ubb_code\">$1</div>"},
                    {"\\[quote\\]([^\\[]*?)\\[\\/quote\\]", "<div class=\"ubb_quote\">$1</div>"},
                    {"\\[color=([^\\]]*)\\]([^\\[]*?)\\[\\/color\\]", "<font style=\"color: $1\">$2</font>"},
                    {"\\[hilitecolor=([^\\]]*)\\]([^\\[]*?)\\[\\/hilitecolor\\]", "<font style=\"background-color: $1\">$2</font>"},
                    {"\\[align=([^\\]]*)\\]([^\\[]*?)\\[\\/align\\]", "<div style=\"text-align: $1\">$2</div>"},
                    {"\\[url=([^\\]]*)\\]([^\\[]*?)\\[\\/url\\]", ""},//,"<a href=\"$1\" target=\"_blank\">$2</a>"},
                    {"\\[img\\]([^\\[]*?)\\[\\/img\\]", ""}//"<a href=\"$1\" target=\"_blank\"><img src=\"$1\" onload=\"cls.img.tResize(this, 600, 1800);\" /></a>"}
            };
            while (tState)
            {
                tState = false;
                for (int ti = 0; ti < tRegexAry.length; ti ++)
                {
                    String tvalue1, tvalue2;
                    Pattern tPattern = Pattern.compile(tRegexAry[ti][0]);
                    Matcher tMatcher = tPattern.matcher(tString);
                    while (tMatcher.find())
                    {
                        tState = true;
                        tvalue1 = tMatcher.group();
                        tvalue2 = tRegexAry[ti][1];
                        for (int tk = 1; tk < (tMatcher.groupCount() + 1); tk ++) tvalue2 = tvalue2.replace("$" + tk, tMatcher.group(tk));
                        tString = tString.replace(tvalue1, tvalue2);
                    }
                }
            }
        }
        return tString;
    }
    
    /**
     * 获取登录密码强度,并返回其位数
     * @param value
     * @param pwdType 0:登录密码 1:资金密码
     * @return
     */
    public static int getBinaryIndexPwdFlag(int value,int pwdType){
    	if(pwdType==0){
    		for(int i=1;i<=3;i++){
        		if(StringUtil.getBinaryIndex(value, i)==1){
        			return i;
        		}
        	}
    	}else{
    		for(int i=4;i<=6;i++){
        		if(StringUtil.getBinaryIndex(value, i)==1){
        			return i;
        		}
        	}
    	}
    	return 0;
    }
    
    /**
     * 设置登录密码强度,并返回其新值
     * @param value
     * @param pwdType 0:登录密码 1:资金密码
     * @return
     */
    public static int setBinaryIndexPwdFlag(int value,int bit,int index,int pwdType){
    	if(pwdType==0){
	    	for(int i=1;i<=3;i++){
	    		value = StringUtil.setBinaryIndex(value,i,index^1);
	    	}
    	}else{
    		for(int i=4;i<=6;i++){
        		value = StringUtil.setBinaryIndex(value,i,index^1);
        	}
    	}
    	return StringUtil.setBinaryIndex(value, bit, index);
    }
    
    /**
     * 检查密码强度
     */
    public static int checkPasswordStrong(String password){
    	int modes=0;
    	for(int i=0;i<password.length();i++){
    		//测试每一个字符的类别并统计一共有多少种模式
    		modes|=charMode(password.charAt(i));
    	}
    	return bitTotal(modes);
    }
    
    /**
     * 验证字符是属于哪一种类型
     */
    public static int charMode(char c){
    	if (c >= 48 && c <= 57) return 1;//数字
    	if (c >= 65 && c <= 90) return 2;//大写字母
    	if (c >= 97 && c <= 122) return 4;//小写
    	else return 8; //特殊字符
    }
    
    /**
     * 统计字符类型总数
     */
    public static int bitTotal(int num){
    	int count = 0;
		for(int i=0;i<4;i++){
			if((num & 1) == 1){
				count++;
			}
			num>>>=1;
		}
    	return count;
    }
    
    /**
     * 上传文件是否为允许的后辍名
     */
    public static Boolean isAllowExt(String fileName){
    	String[] allowFileExt = new String[]{".jpg",".jpeg",".png"};
    	String suffix = fileName.toLowerCase();
    	for(String ext : allowFileExt){
    		if(suffix.endsWith(ext)){
    			return true;
    		}
    	}
    	return false;
    }
    
    /**
     * 格式化数字 
     * @param value
     * @return
     */
    public static String doubleDecimalString(double value){
    	 NumberFormat formatter5 = NumberFormat.getCurrencyInstance(Locale.CHINA);
    	 String str =  formatter5.format(value);
    	 if(!isEmpty(str)){
    		 if(str.contains("￥")||str.contains("$")){
    			 return str.substring(1,str.length());
    		 }
    	 }
    	 return "";
	}
    public static String fixpoint(String value,int size,int intsize){
        String s = value;
        String str = "";
        int p = s.length() - s.indexOf(".")-1;
        if(s.indexOf(".")>0&&p<size){
            for(int i=0;i<size-p;i++){
                str+="0";
            }
        }
        if(s.indexOf(".")<=0){
            str +=".";
            for(int i=0;i<size;i++){
                str+="0";
            }
        }
        String str1 = "";
//        int q = s.indexOf(".");
//        if(q>0&&q<intsize){
//            for(int i=0;i<intsize-q;i++){
//                str1+="&nbsp;&nbsp;";
//            }
//        }
//        if(q<=0){
//            int len = s.length();
//            for(int i=0;i<intsize-len;i++){
//                str1+="&nbsp;&nbsp;";
//            }
//        }
        return str1+value+str;
    }

    /**
     * 精确度随整数部分大小改变
     * @param value
     * @return
     */
    public static String flexibleNumber(double value){
        if(value<1&&value>-1){
            return roundThousandsNumber(value,4);
        }else if((value>=1&&value<10)||(value<=-1&&value>-10)){
            return roundThousandsNumber(value,3);
        }else{
            return roundThousandsNumber(value,2);
        }
    }
    public static String roundThousandsNumber(double value,int size){
        return doubleFormat(DoubleUtil.round(value,size),size);
    }
    /**
     * 验证是否为有效身份证号
     * @param idnumber
     * @return
     */
    public static boolean isIdNumber(String idnumber){
    	if(!StringUtil.isEmpty(idnumber)){
    		String regex = "([1-9]\\d{7}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}|[1-9]\\d{5}[1-9]\\d{3}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}(\\d|[Xx]))";
    		return idnumber.trim().matches(regex);
    	}
    	return false;
    }
    public static boolean isNumber(String number){
    	if(!StringUtil.isEmpty(number)){
    		 Pattern pattern = Pattern.compile("[0-9]*"); 
    		 return pattern.matcher(number).matches(); 
    	}
    	return false;
    }
    /**
     * 
     * @param value
     * @param rate 保留小数后位数 -- 默认8
     * @return
     */
    public static String satoshiToBtcString(double value, int rate){
		if(value == 0)
			return "0";
		else{
			double result = value/100000000.0;
			return doubleFormat(result,rate);
		}
	}

    /**
     * 根据传入的数字进行位移运算
     * @param scale
     * @return
     */
    public static long getOffsetNum(long scale){
        Long temp = new Long(scale);
        temp = temp + 2048L;
        temp = temp << 2;
        temp = ~temp;
        temp = temp << 2;
        temp = temp - 2048L;
        temp = ~temp;
        StringBuffer str = new StringBuffer(temp+"");
        int t = 0;
        for(int i=11 - str.length();i>0;i--){
            str.append(0);
            t++;
        }
        str.append(t);
        return Long.parseLong(str.toString());
    }

    /**
     * 根据传入的数字做反位移运算
     * 限定传入的数字长度为12位大于0的长整型数字
     * @param offsetNum
     * @return
     */
    public static long getOriginNum(long offsetNum) throws Exception{
        StringBuffer str = new StringBuffer(offsetNum+"");
        if(str.length()!=12 || offsetNum<=0){//当前限定长度为12位
            throw new Exception("Argument is illegal.");
        }
        int affix = Integer.parseInt(str.substring(str.length() - 1));
        int length = str.length()-1-affix;
        long finalOffset = Long.parseLong(str.substring(0, length));
        Long temp = new Long(finalOffset);
        temp = ~temp;
        temp = temp + 2048L;
        temp = temp >> 2;
        temp = ~temp;
        temp = temp >> 2;
        temp = temp - 2048L;
        return temp;
    }

    /**
     * 是否过滤登陆ip
     * @return 
     */
    public static boolean isFilterLoginIp(String ip){
    	 
    	if(!StringUtil.isEmpty(ip) && ip.length() >10 && !"218.60.".equals(ip.substring(0, 7)) && !"180.97.".equals(ip.substring(0, 7)) && !"120.24.229.124".equals(ip) && !"43.230.88.".equals(ip.substring(0, 10))){
    		return true;
    	}
    	return false;
    }

    /**
     * null "" 格式化为""
     *
     * @param value
     * @return ""
     */
    public static String stringBlank(String value) {
        if (value == null || value.equals("")) {
            value = "";
        }
        return value.replaceAll("\r|\n", "");
    }

    /**
     * 抽取ubb字符串中的图片
     */
	public static String extarctImg(String str) {
		str = str.replace("&#47;", "/");
    	int off1 = str.indexOf("[img]")+5;
    	int off2 = str.indexOf("[/img]");
		return str.substring(off1, off2);
	}
	
	/**
	 * 手机中间四位替换为 * 
	 * @param str
	 * @return
	 */
	public static String hidePhone(String phone){
		return phone.replaceAll("(\\d{3})\\d{4}(\\d{4})","$1****$2");
	} 
	
	/**
	 * 从身份证号码中提取出生日期
	 */
	public static String getBirthDatefromIdNumber(String idNumber){
		StringBuilder birthDate = new StringBuilder();
		if(isEmpty(idNumber)){
			return "";
		}
		
		if(idNumber.length()==18){
			birthDate = birthDate.append(idNumber.substring(6, 10)).append("-").append(idNumber.substring(10, 12)).append("-").append(idNumber.substring(12, 14));
		}else if(idNumber.length()==15){
			birthDate = birthDate.append("19").append(idNumber.substring(6, 8)).append("-").append(idNumber.substring(8, 10)).append("-").append(idNumber.substring(10, 12));
		}else{
			birthDate = birthDate.append("");
		}
		return birthDate.toString();
	}
	
	/**
	 * 截取银行卡的尾号
	 */
	public static String getTailOfBankCard(String bankCard){
		if(isEmpty(bankCard)){
			return "";
		}
		// 去除空格
		bankCard = bankCard.replaceAll("&nbsp;", "");
		bankCard = bankCard.replaceAll(" ", "");
		
		if(bankCard.length()<=4){
			return bankCard;
		}
		
		bankCard = bankCard.substring(bankCard.length()-4, bankCard.length());
		
		return bankCard;
	}

	/**
	 * 填充字符串: 	1、如果 size<=0 直接返回源字符串
	 * 				2、返回的结果字符串为null 或者 长度等于size
	 * 				3、指定填充位置，如果不指定，默认为左填充
	 * @param source	源字符串
	 * @param size		填充后字符串的总长度
	 * @param padStr	填充的字符
	 * @param position	0左填充  1右填充
     * @return
     */
	public static String pad(String source,int size,String padStr,int position){
		if (source == null) {
			source = "";
		}
		if (isEmpty(padStr)) {
			padStr = " ";
		}
		int padLen = padStr.length();
		int pads = size - source.length();
		if (pads <= 0) {
			return source;
		}
		if (pads < padLen) {
			padStr = padStr.substring(0, pads);
		} else {
			char[] padding = new char[pads];
			char[] padChars = padStr.toCharArray();
			for (int i = 0; i < pads; i++) {
				padding[i] = padChars[i % padLen];
			}
			padStr = new String(padding);
		}
		if(position == 1){
			return source.concat(padStr);
		}else {
			return padStr.concat(source);
		}
	}

	public static void main(String[] args) {
		
		System.out.println(getBirthDatefromIdNumber("370802940221002"));
	}
	
	
}

