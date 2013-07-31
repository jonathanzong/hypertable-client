package com.didactic.htclient.mutator;

import java.io.StringWriter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class TableSchema {

	private Document sch;
	private Element rootElement;

	public TableSchema() throws TransformerException, ParserConfigurationException{
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

		sch = docBuilder.newDocument();
		rootElement = sch.createElement("Schema");
		sch.appendChild(rootElement);
	}

	public TableSchema addAccessGroup(String name){
		Element ag = sch.createElement("AccessGroup");
		rootElement.appendChild(ag);

		ag.setAttribute("name", name == null ? "default" : name);
		return this;
	}

	public TableSchema addColumnFamily(String name){
		return addColumnFamily(name, false, null);
	}
	
	public TableSchema addColumnFamilyAtomic(String name){
		return addColumnFamily(name, true, null);
	}

	public TableSchema addColumnFamily(String name, Boolean counter, String accessGroup){
		Element colfam = sch.createElement("ColumnFamily");
		NodeList accessgroups = rootElement.getElementsByTagName("AccessGroup");
		if(accessGroup == null)
			accessGroup = "default";
		if(accessgroups.getLength() == 0){
			addAccessGroup("default");
			accessgroups = rootElement.getElementsByTagName("AccessGroup");
		}
		for(int i=0;i<accessgroups.getLength();i++){
			Element ag = (Element)accessgroups.item(i);
			if(ag.getAttribute("name").equals(accessGroup)){
				ag.appendChild(colfam);	
				break;
			}
		}

		Element nm = sch.createElement("Name");
		nm.appendChild(sch.createTextNode(name));
		colfam.appendChild(nm);
		
		Element cn = sch.createElement("Counter");
		cn.appendChild(sch.createTextNode(counter.toString()));
		colfam.appendChild(cn);
		
		Element de = sch.createElement("deleted");
		de.appendChild(sch.createTextNode("false"));
		colfam.appendChild(de);
		
		return this;
	}

	@Override
	public String toString(){
		try {

			StringWriter sw = new StringWriter();
			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = tf.newTransformer();

			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			transformer.setOutputProperty(OutputKeys.METHOD, "xml");
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			
			transformer.transform(new DOMSource(sch), new StreamResult(sw));

			return sw.toString();
			
		} catch (TransformerConfigurationException e1) {
			e1.printStackTrace();
		} catch (TransformerException e) {
			e.printStackTrace();
		}
		return super.toString();
	}
	
}
