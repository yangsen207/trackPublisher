package trackPublisher;

import java.io.*;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import javax.xml.bind.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class TrackPublisher {

	private static final String TOPIC_PREFIX = "cag/sin/track/v1/"; // used as the topic "root"
	private static volatile boolean isShutdown = false;

	@SuppressWarnings("restriction")
	public static void main(String[] args) throws JCSMPException, IOException, JAXBException  {
		if (args.length < 5) { // Check command line arguments
			System.out.println("Usage: <file-path> <host:port> <message-vpn> <client-username> <password> [msg-rate]");
			System.exit(-1);
		}
		String filePath = args[0];
		final JCSMPProperties properties = new JCSMPProperties();
		properties.setProperty(JCSMPProperties.HOST, args[1]); // host:port
		properties.setProperty(JCSMPProperties.VPN_NAME, args[2]); // message-vpn
		properties.setProperty(JCSMPProperties.USERNAME, args[3]); // client-username
		properties.setProperty(JCSMPProperties.PASSWORD, args[4]); // client-password
		int msg_rate = 5;
		if (args.length > 5) {
			msg_rate = Integer.parseInt(args[5]);
		}
		properties.setProperty(JCSMPProperties.GENERATE_SEQUENCE_NUMBERS, true); // not required, but interesting
		JCSMPChannelProperties channelProps = new JCSMPChannelProperties();
		channelProps.setReconnectRetries(20); // recommended settings
		channelProps.setConnectRetriesPerHost(5); // recommended settings
		// https://docs.solace.com/Solace-PubSub-Messaging-APIs/API-Developer-Guide/Configuring-Connection-T.htm
		properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, channelProps);
		final JCSMPSession session;

			session = JCSMPFactory.onlyInstance().createSession(properties, null, new SessionEventHandler() {
				@Override
				public void handleEvent(SessionEventArgs event) { // could be reconnecting, connection lost, etc.
					System.out.printf("### Received a Session event: %s%n", event);
				}
			});
			session.connect(); // connect to the broker
			// Simple anonymous inner-class for handling publishing events
			final XMLMessageProducer producer;
			producer = session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
				// unused in Direct Messaging application, only for Guaranteed/Persistent
				// publishing application
				@Override
				public void responseReceived(String key) {
				}
				@Override
				public void responseReceivedEx(Object key) {
				}
				
				@Override
				public void handleError(String key, JCSMPException cause, long timestamp) {
					
				}
				// can be called for ACL violations, connection loss, and Persistent NACKs
				@Override
				public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
					System.out.printf("### Producer handleErrorEx() callback: %s%n", cause);
					if (cause instanceof JCSMPTransportException) { // all reconnect attempts failed
						isShutdown = true; // let's quit; or, could initiate a new connection attempt
					} else if (cause instanceof JCSMPErrorResponseException) { // might have some extra info
						JCSMPErrorResponseException e = (JCSMPErrorResponseException) cause;
						System.out.println(JCSMPErrorResponseSubcodeEx.getSubcodeAsString(e.getSubcodeEx()) + ": "
								+ e.getResponsePhrase());
						System.out.println(cause);
					}
				}
			});
			final BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);

			BufferedReader br = new BufferedReader(new FileReader(filePath));

			String str = "";

			int sentCount = 0;
			JAXBContext jc = JAXBContext.newInstance(TRACK.class);
			Unmarshaller unmarshaller = jc.createUnmarshaller();

			ObjectMapper mapper = new ObjectMapper();
			
			while (str != null && !isShutdown) {
				str = br.readLine();
				
				//reached EOF, loop the file again
				if(str == null) {
					br = new BufferedReader(new FileReader(filePath));
					str = br.readLine();
				}
				String s = str.trim();
				if (s.length() < 10)
					continue;
				if (!s.startsWith("<TRACK>"))
					continue;
				
				TRACK track;
				try {
				InputStream ins = new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
				track = (TRACK) unmarshaller.unmarshal(ins);
				} catch(JAXBException e ) {
					System.out.println(s);
					continue;
				}
				
				String trackType = track.tracktype;
				BigInteger trackNumber = track.tracknumber;
				float lat = track.latitude;
				float lon = track.longitude;
				String json = mapper.writeValueAsString(track);

				// System.out.println(json);
				message.setData(json.getBytes());
				String topicString = new StringBuilder(TOPIC_PREFIX).append(trackType).append("/").append(trackNumber).append("/").append(lat).append("/")
						.append(lon).toString(); // StringBuilder faster than +
				producer.send(message, JCSMPFactory.onlyInstance().createTopic(topicString)); // send the message
				message.reset(); // reuse this message, to avoid having to recreate it: better performance

				sentCount++;
				if (sentCount == msg_rate) {
					sentCount = 0;
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						continue;
					}
				}
			}
		}
}
