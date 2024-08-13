/* The convention code for creating the connection object to all code should go here
Eventually all the config for this part of the package should move to spring */

package org.example;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.ConnectionFactory;

/* need to figure out how to structyure the different connection objects (ACTIVEMQ, EMS, RV, KAFKA) */
public class JMSConfig {

    public static ConnectionFactory connectionFactory() {
        return new ActiveMQConnectionFactory("tcp://localhost:61616");
    }
}