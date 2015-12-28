/*
 * Copyright 2013 Nick Hecht chilliwebs@gmail.com.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chilliwebs.ezjdo;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.activation.DataSource;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.ws.BindingType;
import javax.xml.ws.Provider;
import javax.xml.ws.Service;
import javax.xml.ws.ServiceMode;
import javax.xml.ws.WebServiceContext;
import javax.xml.ws.WebServiceException;
import javax.xml.ws.WebServiceProvider;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.http.HTTPBinding;

/**
 * @author Nick Hecht chilliwebs@gmail.com
 */
@WebServiceProvider(serviceName = "ezjdo")
@BindingType(value = HTTPBinding.HTTP_BINDING)
@ServiceMode(value = Service.Mode.MESSAGE)
/* package */ class ezJDOServiceProvider implements Provider<DataSource> {

    private JAXBContext jc;
    @javax.annotation.Resource()
    protected WebServiceContext wsContext;

    public ezJDOServiceProvider() {
        Enumeration<URL> systemResources = null;
        try {
            systemResources = getClass().getClassLoader().getResources("META-INF");
        } catch (IOException ex) {
            Logger.getLogger(ezJDOServiceProvider.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (systemResources != null) {
            while (systemResources.hasMoreElements()) {
                URL nextElement = systemResources.nextElement();
                Object resource;
                try {
                    if ("jar".equals(nextElement.getProtocol())) {
                        try {
                            resource = ((JarURLConnection) nextElement.openConnection()).getJarFile();
                        } catch (IOException ex) {
                            resource = new File(new URI(nextElement.toString().split("!", 2)[0]));
                        }
                    } else {
                        resource = new File(new URI(nextElement.toString().split("!", 2)[0]));
                    }
                } catch (IllegalArgumentException ex) {
                    resource = new File(nextElement.toString().split("!", 2)[0]);
                } catch (URISyntaxException ex) {
                    resource = new File(nextElement.toString().split("!", 2)[0]);
                }

                //System.out.println("++" + resource);
                if (resource instanceof File) {
                    String[] list = ((File) resource).list();
                    if (list != null) {
                        for (String name : list) {
                            try {
                                if (Class.forName(name).isAssignableFrom(BaseObject.class)) {
                                    System.out.println("  ++" + name);
                                }
                            } catch (ClassNotFoundException ex) {
                            }
                        }
                    }
                } else if (resource instanceof JarFile) {
                    Enumeration<JarEntry> entries = ((JarFile) resource).entries();
                    while (entries.hasMoreElements()) {
                        String name = entries.nextElement().getName();
                        try {
                            if (Class.forName(name).isAssignableFrom(BaseObject.class)) {
                                System.out.println("  ++" + name);
                            }
                        } catch (ClassNotFoundException ex) {
                        }
                    }
                }
            }
        }

        try {
            jc = JAXBContext.newInstance(BaseObject.class);
        } catch (JAXBException je) {
            throw new WebServiceException("Cannot create JAXBContext", je);
        }
    }

    @Override
    public DataSource invoke(DataSource request) {
        System.out.println(request.getContentType());
        try {
            MessageContext mc = wsContext.getMessageContext();
            String path = (String) mc.get(MessageContext.PATH_INFO);
            String method = (String) mc.get(MessageContext.HTTP_REQUEST_METHOD);
            if (method.equals("GET")) {
                return get(mc);
            }
            if (method.equals("POST")) {
                return post(request, mc);
            }
            if (method.equals("PUT")) {
                return put(request, mc);
            }
            if (method.equals("DELETE")) {
                return delete(request, mc);
            }
            throw new WebServiceException("Unsupported method:" + method);
        } catch (JAXBException je) {
            throw new WebServiceException(je);
        }
    }
//        Source message = null;
//        try {
//            JAXBContext jc = JAXBContext.newInstance();
//            Unmarshaller u = jc.createUnmarshaller();
//            BaseObject o = (BaseObject) u.unmarshal(message.getSOAPBody().extractContentAsDocument(), BaseObject.class.);
//            BaseObject ba = BaseObject.find(BaseObject.class,"7686");
//
//            jc = JAXBContext.newInstance(BaseObject.class);
//            Marshaller m = jc.createMarshaller();
//            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
//            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
//            dbf.setNamespaceAware(true);
//            Document doc = dbf.newDocumentBuilder().newDocument();
//            m.marshal(ba, doc);
//            Element createElement = doc.createElementNS("ws", "getAgentByCodeResponse");
//            createElement.appendChild(doc.getDocumentElement());
//            doc.appendChild(createElement);
//            MessageFactory factory = MessageFactory.newInstance();
//            message = factory.createMessage();
//            message.getSOAPBody().addDocument(doc);
//            message.saveChanges();
//
//        } catch (JAXBException ex) {
//            Logger.getLogger(ezJDOServiceProvider.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (ParserConfigurationException ex) {
//            Logger.getLogger(ezJDOServiceProvider.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (SOAPException ex) {
//            Logger.getLogger(ezJDOServiceProvider.class.getName()).log(Level.SEVERE, null, ex);
//        }
    //        try {
//            String amount = null;
//
//            if (source == null) {
//                System.out.println("Getting input from query string");
//                MessageContext mc = wsContext.getMessageContext();
//                String query = (String) mc.get(MessageContext.QUERY_STRING);
//                System.out.println("Query String = " + query);
//                ServletRequest req = (ServletRequest) mc.get(MessageContext.SERVLET_REQUEST);
//                amount = req.getParameter("amount");
//            } else {
//                System.out.println("Getting input from input message");
//                Node n = null;
//                if (source instanceof DOMSource) {
//                    n = ((DOMSource) source).getNode();
//                } else if (source instanceof StreamSource) {
//                    StreamSource streamSource = (StreamSource) source;
//                    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
//                    DocumentBuilder db = dbf.newDocumentBuilder();
//                    InputSource inputSource = null;
//                    if (streamSource.getInputStream() != null) {
//                        inputSource = new InputSource(streamSource.getInputStream());
//                    } else if (streamSource.getReader() != null) {
//                        inputSource = new InputSource(streamSource.getReader());
//                    }
//                    n = db.parse(inputSource);
//                } else {
//                    throw new RuntimeException("Unsupported source: " + source);
//                }
//                NodeList children = n.getChildNodes();
//                for (int i = 0; i < children.getLength(); i++) {
//                    Node child = children.item(i);
//                    if (child.getNodeName().equals("dollars")) {
//                        amount = child.getAttributes().getNamedItem("amount").getNodeValue();
//                        break;
//                    }
//                }
//            }
//            BigDecimal dollars = new BigDecimal(amount);
//            BigDecimal rupees = dollarToRupees(dollars);
//            BigDecimal euros = rupeesToEuro(rupees);
//            return createResultSource(rupees, euros);
//        } catch (Exception e) {
//            e.printStackTrace();
//            throw new HTTPException(500);
//        }
//    }
    private BigDecimal rupeeRate = new BigDecimal("40.58");
    private BigDecimal euroRate = new BigDecimal("0.018368");

    public BigDecimal dollarToRupees(BigDecimal dollars) {
        BigDecimal result = dollars.multiply(rupeeRate);
        return result.setScale(2, BigDecimal.ROUND_UP);
    }

    public BigDecimal rupeesToEuro(BigDecimal rupees) {
        BigDecimal result = rupees.multiply(euroRate);
        return result.setScale(2, BigDecimal.ROUND_UP);
    }

    private Source createResultSource(BigDecimal rupees, BigDecimal euros) {
        String body = "<ns:return xmlns:ns=\"http://converterservice.org\">"
                + "<ns:dollarToRupeesResponse>" + rupees + "</ns:dollarToRupeesResponse><ns:rupeesToEurosResponse>"
                + euros + "</ns:rupeesToEurosResponse></ns:return>";
        body = "TEST";
        Source source = new StreamSource(new ByteArrayInputStream(body.getBytes()));
        return source;
    }

    private DataSource get(MessageContext mc) throws JAXBException {
        return new DataSource() {
            @Override
            public InputStream getInputStream() {
                return new ByteArrayInputStream("{\"value\":\"test\"}".getBytes());
            }

            @Override
            public OutputStream getOutputStream() {
                return null;
            }

            @Override
            public String getContentType() {
                return "application/json";
            }

            @Override
            public String getName() {
                return "";
            }
        };
    }

    private DataSource post(DataSource request, MessageContext mc) throws JAXBException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private DataSource put(DataSource request, MessageContext mc) throws JAXBException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private DataSource delete(DataSource request, MessageContext mc) throws JAXBException {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}