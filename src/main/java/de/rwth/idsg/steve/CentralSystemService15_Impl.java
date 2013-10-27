package de.rwth.idsg.steve;

import java.sql.Timestamp;
import java.util.List;

import javax.annotation.Resource;
import javax.xml.ws.WebServiceContext;
import javax.xml.ws.handler.MessageContext;

import ocpp.cs._2012._06.AuthorizationStatus;
import ocpp.cs._2012._06.AuthorizeRequest;
import ocpp.cs._2012._06.AuthorizeResponse;
import ocpp.cs._2012._06.BootNotificationRequest;
import ocpp.cs._2012._06.BootNotificationResponse;
import ocpp.cs._2012._06.CentralSystemService;
import ocpp.cs._2012._06.DataTransferRequest;
import ocpp.cs._2012._06.DataTransferResponse;
import ocpp.cs._2012._06.DiagnosticsStatusNotificationRequest;
import ocpp.cs._2012._06.DiagnosticsStatusNotificationResponse;
import ocpp.cs._2012._06.FirmwareStatusNotificationRequest;
import ocpp.cs._2012._06.FirmwareStatusNotificationResponse;
import ocpp.cs._2012._06.HeartbeatRequest;
import ocpp.cs._2012._06.HeartbeatResponse;
import ocpp.cs._2012._06.IdTagInfo;
import ocpp.cs._2012._06.MeterValue;
import ocpp.cs._2012._06.MeterValuesRequest;
import ocpp.cs._2012._06.MeterValuesResponse;
import ocpp.cs._2012._06.RegistrationStatus;
import ocpp.cs._2012._06.StartTransactionRequest;
import ocpp.cs._2012._06.StartTransactionResponse;
import ocpp.cs._2012._06.StatusNotificationRequest;
import ocpp.cs._2012._06.StatusNotificationResponse;
import ocpp.cs._2012._06.StopTransactionRequest;
import ocpp.cs._2012._06.StopTransactionResponse;
import ocpp.cs._2012._06.TransactionData;

import org.apache.cxf.ws.addressing.AddressingProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.rwth.idsg.steve.common.Constants;
import de.rwth.idsg.steve.common.SQLIdTagData;
import de.rwth.idsg.steve.common.ServiceDBAccess;
import de.rwth.idsg.steve.common.Utils;

/**
 * Service implementation of OCPP V1.5
 * 
 * @author Sevket Goekay <goekay@dbis.rwth-aachen.de>
 *  
 */
@javax.jws.WebService(
		serviceName = "CentralSystemService",
		portName = "CentralSystemServiceSoap12",
		targetNamespace = "urn://Ocpp/Cs/2012/06/",
		wsdlLocation = "file:/Users/sgokay/git/steve/src/main/webapp/ocpp_centralsystemservice_1.5_final.wsdl",
		endpointInterface = "ocpp.cs._2012._06.CentralSystemService")

public class CentralSystemService15_Impl implements CentralSystemService {
	@Resource
	private WebServiceContext webServiceContext;

	private static final Logger LOG = LoggerFactory.getLogger(CentralSystemService15_Impl.class);
	
	public BootNotificationResponse bootNotification(BootNotificationRequest parameters,java.lang.String chargeBoxIdentity) { 
		LOG.info("Executing bootNotification for {}", chargeBoxIdentity);
		
		// Get the Address value from WS-A Header
		MessageContext messageContext = webServiceContext.getMessageContext();
		AddressingProperties addressProp = (AddressingProperties) messageContext.get(org.apache.cxf.ws.addressing.JAXWSAConstants.SERVER_ADDRESSING_PROPERTIES_INBOUND);
		String endpoint_address = addressProp.getFrom().getAddress().getValue();
		
		boolean isRegistered = ServiceDBAccess.updateChargebox(endpoint_address,
				"1.5",
				parameters.getChargePointVendor(),
				parameters.getChargePointModel(),
				parameters.getChargePointSerialNumber(),
				parameters.getChargeBoxSerialNumber(),
				parameters.getFirmwareVersion(),
				parameters.getIccid(),
				parameters.getImsi(),
				parameters.getMeterType(),
				parameters.getMeterSerialNumber(),
				chargeBoxIdentity);
		
		BootNotificationResponse _return = new BootNotificationResponse();
		RegistrationStatus _returnStatus = null;
		
		if (isRegistered) {
			_returnStatus = RegistrationStatus.ACCEPTED;
			_return.setCurrentTime(Utils.getCurrentDateTimeXML());
			_return.setHeartbeatInterval(Integer.valueOf(Constants.HEARTBEAT_INTERVAL));
		} else {
			_returnStatus = RegistrationStatus.REJECTED;
		}		
		_return.setStatus(_returnStatus);		
		return _return;
	}
	
	public FirmwareStatusNotificationResponse firmwareStatusNotification(FirmwareStatusNotificationRequest parameters,java.lang.String chargeBoxIdentity) { 
		LOG.info("Executing firmwareStatusNotification for {}", chargeBoxIdentity);

		String status = parameters.getStatus().toString();
		ServiceDBAccess.updateChargeboxFirmwareStatus(chargeBoxIdentity, status);
		
		FirmwareStatusNotificationResponse _return = new FirmwareStatusNotificationResponse();
		return _return;
	}	

	public StatusNotificationResponse statusNotification(StatusNotificationRequest parameters,java.lang.String chargeBoxIdentity) { 
		LOG.info("Executing statusNotification for {}", chargeBoxIdentity);

		// Mandatory fields
		int connectorId = parameters.getConnectorId();
		String status = parameters.getStatus().toString();
		String errorCode = parameters.getErrorCode().toString();
		
		// Optional fields
		String errorInfo = parameters.getInfo();
		Timestamp timeStamp = Utils.convertToTimestamp(parameters.getTimestamp());
		String vendorId = parameters.getVendorId();
		String vendorErrorCode = parameters.getVendorErrorCode();
		
		ServiceDBAccess.insertConnectorStatus(chargeBoxIdentity, connectorId, status, timeStamp, errorCode, errorInfo, vendorId, vendorErrorCode);
		
		StatusNotificationResponse _return = new StatusNotificationResponse();
		return _return;
	}

	public MeterValuesResponse meterValues(MeterValuesRequest parameters,java.lang.String chargeBoxIdentity) { 
		LOG.info("Executing meterValues for {}", chargeBoxIdentity);

		int connectorId = parameters.getConnectorId();
		Integer transactionId = parameters.getTransactionId();	
		List<MeterValue> valuesList = parameters.getValues();
		
		if (valuesList != null) {
			ServiceDBAccess.insertMeterValues15(chargeBoxIdentity, connectorId, transactionId, valuesList);
		}
	
		MeterValuesResponse _return = new MeterValuesResponse();
		return _return;
	}

	public DiagnosticsStatusNotificationResponse diagnosticsStatusNotification(DiagnosticsStatusNotificationRequest parameters,java.lang.String chargeBoxIdentity) { 
		LOG.info("Executing diagnosticsStatusNotification for {}", chargeBoxIdentity);
		
		String status = parameters.getStatus().toString();
		ServiceDBAccess.updateChargeboxDiagnosticsStatus(chargeBoxIdentity, status);
		
		DiagnosticsStatusNotificationResponse _return = new DiagnosticsStatusNotificationResponse();
		return _return;
	}
	
	public StartTransactionResponse startTransaction(StartTransactionRequest parameters,java.lang.String chargeBoxIdentity) { 
		LOG.info("Executing startTransaction for {}", chargeBoxIdentity);

		// Get the authorization info of the user
		String idTag = parameters.getIdTag();		
		SQLIdTagData sqlData = ServiceDBAccess.getIdTagColumns(idTag);
		IdTagInfo _returnIdTagInfo = createIdTagInfo(sqlData);
		
		int transactionId = -1;
		if (_returnIdTagInfo.getStatus() == AuthorizationStatus.ACCEPTED){
			
			// Get parameters and insert transaction to DB
			int connectorId = parameters.getConnectorId();
			Integer reservationId = parameters.getReservationId();
			Timestamp startTimestamp = Utils.convertToTimestamp(parameters.getTimestamp());
			String startMeterValue = Integer.toString(parameters.getMeterStart());
			transactionId = ServiceDBAccess.insertTransaction(chargeBoxIdentity, connectorId, idTag, startTimestamp, startMeterValue, reservationId);			
		}
		
		StartTransactionResponse _return = new StartTransactionResponse();
		_return.setIdTagInfo(_returnIdTagInfo);
		if (transactionId != -1) { _return.setTransactionId(transactionId); }
		return _return;
	}
	
	public StopTransactionResponse stopTransaction(StopTransactionRequest parameters,java.lang.String chargeBoxIdentity) { 
		LOG.info("Executing stopTransaction for {}", chargeBoxIdentity);

		// Get parameters and update transaction in DB
		int transactionId = parameters.getTransactionId();
		Timestamp stopTimestamp = Utils.convertToTimestamp(parameters.getTimestamp());
		String stopMeterValue = Integer.toString(parameters.getMeterStop());		
		ServiceDBAccess.updateTransaction(chargeBoxIdentity, transactionId, stopTimestamp, stopMeterValue);
		
		// Get the connectorId that is used for transactionId, and insert meter values
		List<TransactionData> transDataList = parameters.getTransactionData();
		if (transDataList != null){
			for (TransactionData temp : transDataList) {
				int connectorId = ServiceDBAccess.getConnectorId(transactionId);
				if (connectorId != -1) {
					ServiceDBAccess.insertMeterValues15(chargeBoxIdentity, connectorId, transactionId, temp.getValues());
				}
			}
		}
		
		// Get the authorization info of the user
		StopTransactionResponse _return = new StopTransactionResponse();
		String idTag = parameters.getIdTag();
		if (!idTag.isEmpty()) {
			SQLIdTagData sqlData = ServiceDBAccess.getIdTagColumns(idTag);
			_return.setIdTagInfo(createIdTagInfo(sqlData));
		}
		return _return;
	}

	public HeartbeatResponse heartbeat(HeartbeatRequest parameters,java.lang.String chargeBoxIdentity) { 
		LOG.info("Executing heartbeat for {}", chargeBoxIdentity);

		HeartbeatResponse _return = new HeartbeatResponse();
		_return.setCurrentTime(Utils.getCurrentDateTimeXML());
		return _return;
	}

	public AuthorizeResponse authorize(AuthorizeRequest parameters,java.lang.String chargeBoxIdentity) { 
		LOG.info("Executing authorize for {}", chargeBoxIdentity);

		// Get the authorization info of the user
		String idTag = parameters.getIdTag();
		SQLIdTagData sqlData = ServiceDBAccess.getIdTagColumns(idTag);

		AuthorizeResponse _return = new AuthorizeResponse();
		_return.setIdTagInfo(createIdTagInfo(sqlData));
		return _return;
	}

	// Dummy implementation. This is new in OCPP 1.5. It must be vendor-specific.
	public DataTransferResponse dataTransfer(DataTransferRequest parameters,java.lang.String chargeBoxIdentity) { 
		LOG.info("Executing dataTransfer for {}", chargeBoxIdentity);
		
		String vendorId = parameters.getVendorId();
		String messageId = parameters.getMessageId();
		String data = parameters.getData();
		
		LOG.info("[Data Transfer] Charge point: {}, Vendor Id: {}", chargeBoxIdentity, vendorId);
		if (!messageId.isEmpty()) LOG.info("[Data Transfer] Message Id: {}", messageId);		
		if (!data.isEmpty()) LOG.info("[Data Transfer] Data: {}", data);
		
		DataTransferResponse _return = new DataTransferResponse();
		//_return.setData(value);
		//_return.setStatus(value);
		return _return;
	}
	
	private static IdTagInfo createIdTagInfo(SQLIdTagData sqlData){
		IdTagInfo _returnIdTagInfo = new IdTagInfo();
		AuthorizationStatus _returnIdTagInfoStatus = null;

		if (sqlData == null) {
			// Id is not in DB (unknown id). Not allowed for charging.
			_returnIdTagInfoStatus = AuthorizationStatus.INVALID;
			LOG.info("The idTag of this user is INVALID (not present in DB).");
		} else {	
			if (sqlData.inTransaction == true) {
				_returnIdTagInfoStatus = AuthorizationStatus.CONCURRENT_TX;
				LOG.info("The idTag of this user is ALREADY in another transaction.");
			} else if (sqlData.blocked == true) {
				_returnIdTagInfoStatus = AuthorizationStatus.BLOCKED;
				LOG.info("The idTag of this user is BLOCKED.");
			} else if (sqlData.expiryDate != null && Utils.getCurrentDateTimeTS().after(sqlData.expiryDate)) {
				_returnIdTagInfoStatus = AuthorizationStatus.EXPIRED;
				LOG.info("The idTag of this user is EXPIRED.");
			} else {
				_returnIdTagInfoStatus = AuthorizationStatus.ACCEPTED;
				// When accepted, set the additional fields
				_returnIdTagInfo.setExpiryDate(Utils.setExpiryDateTime(Constants.HOURS_TO_EXPIRE));
				if ( sqlData.parentIdTag != null ) _returnIdTagInfo.setParentIdTag(sqlData.parentIdTag);
				LOG.info("The idTag of this user is ACCEPTED.");
			}
		}
		_returnIdTagInfo.setStatus(_returnIdTagInfoStatus);
		return _returnIdTagInfo;
	}
}