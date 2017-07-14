package com.recommendengine.compute.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HtmlParser {

	private static final String regEx_script = "<script[^>]*?>[\\s\\S]*?<\\/script>"; // 定义script的正则表达式
	private static final String regEx_style = "<style[^>]*?>[\\s\\S]*?<\\/style>"; // 定义style的正则表达式
	private static final String regEx_html = "<[^>]+>"; // 定义HTML标签的正则表达式
	private static final String regEx_space = "\\s*|\t|\r|\n";//定义空格回车换行符 
	private static final String regEx_punc = "\\&[a-zA-Z]{1,10};";
	/**
	 * @param htmlStr
	 * @return 删除Html标签
	 */
	public static String delHTMLTag(String htmlStr) {
		Pattern p_script = Pattern.compile(regEx_script, Pattern.CASE_INSENSITIVE);
		Matcher m_script = p_script.matcher(htmlStr);
		htmlStr = m_script.replaceAll(""); // 过滤script标签

		Pattern p_style = Pattern.compile(regEx_style, Pattern.CASE_INSENSITIVE);
		Matcher m_style = p_style.matcher(htmlStr);
		htmlStr = m_style.replaceAll(""); // 过滤style标签

		Pattern p_html= Pattern.compile(regEx_html, Pattern.CASE_INSENSITIVE);
		Matcher m_html = p_html.matcher(htmlStr);
		htmlStr = m_html.replaceAll(""); // 过滤html标签  
		
		Pattern p_space = Pattern.compile(regEx_space, Pattern.CASE_INSENSITIVE);
		Matcher m_space = p_space.matcher(htmlStr);
		htmlStr = m_space.replaceAll(""); // 过滤空格回车标签 
		
		Pattern p_punc =Pattern.compile(regEx_punc, Pattern.CASE_INSENSITIVE);
		Matcher m_punc =p_punc.matcher(htmlStr);
		htmlStr = m_punc.replaceAll("");
		
		return htmlStr.trim(); // 返回文本字符串

		// htmlStr = m_space.replaceAll(""); // 过滤空格回车标签
	}

	public static String getTextFromHtml(String htmlStr) {
		htmlStr = delHTMLTag(htmlStr);
		htmlStr = htmlStr.replaceAll(" ", "");
//		htmlStr = htmlStr.substring(0, htmlStr.indexOf("。") + 1);
		return htmlStr;
	}

	public static void main(String[] args) {
		DecimalFormat df = new DecimalFormat("#.00");
		System.out.println(df.format(231.23123));
		
		try {
			File file=new File("/home/hadoop/train/test");
			BufferedReader read=new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			StringBuffer sb=new StringBuffer();
			String tmp="";
			while ((tmp=read.readLine())!=null){
				sb.append(tmp);
			}
			System.out.println(getTextFromHtml(sb.toString()));
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
		}catch (IOException e) {
			
			e.printStackTrace();
		}

	}
}
