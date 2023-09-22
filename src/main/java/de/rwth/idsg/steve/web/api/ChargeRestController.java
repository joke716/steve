/*
 * SteVe - SteckdosenVerwaltung - https://github.com/steve-community/steve
 * Copyright (C) 2013-2023 SteVe Community Team
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package de.rwth.idsg.steve.web.api;

import de.rwth.idsg.steve.ocpp.CommunicationTask;
import de.rwth.idsg.steve.ocpp.RequestResult;
import de.rwth.idsg.steve.repository.ChargePointRepository;
import de.rwth.idsg.steve.repository.TaskStore;
import de.rwth.idsg.steve.repository.dto.ChargePoint;
import de.rwth.idsg.steve.repository.dto.ChargePointSelect;
import de.rwth.idsg.steve.service.ChargePointHelperService;
import de.rwth.idsg.steve.service.ChargePointService16_Client;
import de.rwth.idsg.steve.utils.mapper.ChargePointDetailsMapper;
import de.rwth.idsg.steve.web.api.ApiControllerAdvice.ApiErrorResponse;
import de.rwth.idsg.steve.web.dto.ChargePointForm;
import de.rwth.idsg.steve.web.dto.ocpp.*;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Sevket Goekay <sevketgokay@gmail.com>
 * @since 13.09.2022
 */
@Slf4j
@RestController
@RequestMapping(value = "/api/v1/chargepoints", produces = MediaType.APPLICATION_JSON_VALUE)
@RequiredArgsConstructor
public class ChargeRestController {
    @Autowired private TaskStore taskStore;
    @Autowired
    protected ChargePointRepository chargePointRepository;
    @Autowired protected ChargePointHelperService chargePointHelperService;

    private static final String REMOTE_START_TX_PATH = "/remoteStart";
    private static final String REMOTE_END_TX_PATH = "/remoteEnd";

    private static final String REMOTE_HEARTBEAT_PATH = "/heartbeat";
    private static final String REMOTE_HEARTBEAT_CONFIRM_PATH = "/task/{taskId}";

    @Autowired
    @Qualifier("ChargePointService16_Client")
    private ChargePointService16_Client client16;


    protected static final String PARAMS = "params";

    private static final List<String> upToOcpp15RegistrationStatusList = Arrays.stream(ocpp.cs._2012._06.RegistrationStatus.values())
            .map(ocpp.cs._2012._06.RegistrationStatus::value)
            .collect(Collectors.toList());

    private static final List<String> ocpp16RegistrationStatusList = Arrays.stream(ocpp.cs._2015._10.RegistrationStatus.values())
            .map(ocpp.cs._2015._10.RegistrationStatus::value)
            .collect(Collectors.toList());

    @ApiResponses(value = {
        @ApiResponse(code = 201, message = "Created"),
        @ApiResponse(code = 400, message = "Bad Request", response = ApiErrorResponse.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiErrorResponse.class),
        @ApiResponse(code = 422, message = "Unprocessable Entity", response = ApiErrorResponse.class),
        @ApiResponse(code = 404, message = "Not Found", response = ApiErrorResponse.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiErrorResponse.class)}
    )
    @PostMapping
    @ResponseBody
    @ResponseStatus(HttpStatus.CREATED)
    public ChargePointForm create(@RequestBody @Valid ChargePointForm chargePointForm) {
        log.debug("Create request: {}", chargePointForm);
        int chargeBoxPk = add(chargePointForm);
        ChargePoint.Details cp = chargePointRepository.getDetails(chargeBoxPk);
        ChargePointForm form = ChargePointDetailsMapper.mapToForm(cp);
        return form;
    }

    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request", response = ApiErrorResponse.class),
            @ApiResponse(code = 401, message = "Unauthorized", response = ApiErrorResponse.class),
            @ApiResponse(code = 422, message = "Unprocessable Entity", response = ApiErrorResponse.class),
            @ApiResponse(code = 404, message = "Not Found", response = ApiErrorResponse.class),
            @ApiResponse(code = 500, message = "Internal Server Error", response = ApiErrorResponse.class)}
    )
    @PostMapping(REMOTE_START_TX_PATH)
    @ResponseBody
    public RemoteStartTransactionParamsApi start(@RequestBody @Valid RemoteStartTransactionParamsApi params) {
        RemoteStartTransactionParams request = new RemoteStartTransactionParams();
        ChargePointSelect chargePointSelect = new ChargePointSelect(params.getOcppTransport(), params.getChargeBoxId());
        request.setIdTag(params.getIdTag());
        request.setConnectorId(params.getConnectorId());
        request.setChargePointSelectList(List.of(
                chargePointSelect
        ));
        log.info("Create request: {}", request);
        client16.remoteStartTransaction(request);
        return params;
    }

    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request", response = ApiErrorResponse.class),
            @ApiResponse(code = 401, message = "Unauthorized", response = ApiErrorResponse.class),
            @ApiResponse(code = 422, message = "Unprocessable Entity", response = ApiErrorResponse.class),
            @ApiResponse(code = 404, message = "Not Found", response = ApiErrorResponse.class),
            @ApiResponse(code = 500, message = "Internal Server Error", response = ApiErrorResponse.class)}
    )
    @PostMapping(REMOTE_END_TX_PATH)
    @ResponseBody
    public RemoteStopTransactionParamsApi stop(@RequestBody @Valid RemoteStopTransactionParamsApi params) {
        log.debug("Create request: {}", params);
        RemoteStopTransactionParams request = new RemoteStopTransactionParams();
        ChargePointSelect chargePointSelect = new ChargePointSelect(params.getOcppTransport(), params.getChargeBoxId());
        request.setTransactionId(params.getTransactionId());
        request.setChargePointSelectList(List.of(
                chargePointSelect
        ));
        client16.remoteStopTransaction(request);
        return params;
    }

    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request", response = ApiErrorResponse.class),
            @ApiResponse(code = 401, message = "Unauthorized", response = ApiErrorResponse.class),
            @ApiResponse(code = 422, message = "Unprocessable Entity", response = ApiErrorResponse.class),
            @ApiResponse(code = 404, message = "Not Found", response = ApiErrorResponse.class),
            @ApiResponse(code = 500, message = "Internal Server Error", response = ApiErrorResponse.class)}
    )
    @PostMapping(REMOTE_HEARTBEAT_PATH)
    @ResponseBody
    public RemoteTaskResponse heartBeat(@RequestBody @Valid RemoteHeartBeatRequest params) {
        ChargePointSelect chargePointSelect = new ChargePointSelect(params.getOcppTransport(), params.getChargeBoxId());
        TriggerMessageParams triggerMessageParams = new TriggerMessageParams();
        triggerMessageParams.setTriggerMessage(TriggerMessageEnum.StatusNotification);
        triggerMessageParams.setChargePointSelectList(List.of(chargePointSelect));
        log.info("HeartBeat request: {}", triggerMessageParams);
        RemoteTaskResponse remoteTaskResponse = new RemoteTaskResponse();
        int taskNumber = client16.triggerMessage(triggerMessageParams);
        remoteTaskResponse.setTaskNumber(taskNumber);
        return remoteTaskResponse;
    }

    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request", response = ApiErrorResponse.class),
            @ApiResponse(code = 401, message = "Unauthorized", response = ApiErrorResponse.class),
            @ApiResponse(code = 422, message = "Unprocessable Entity", response = ApiErrorResponse.class),
            @ApiResponse(code = 404, message = "Not Found", response = ApiErrorResponse.class),
            @ApiResponse(code = 500, message = "Internal Server Error", response = ApiErrorResponse.class)}
    )
    @GetMapping(REMOTE_HEARTBEAT_CONFIRM_PATH)
    public Map<String, RequestResult> taskConfirm(@PathVariable("taskId") Integer taskId) {
        CommunicationTask r = taskStore.get(taskId);
        return r.getResultMap();
    }

    private int add(ChargePointForm form) {
        int pk = chargePointRepository.addChargePoint(form);
        chargePointHelperService.removeUnknown(Collections.singletonList(form.getChargeBoxId()));
        return pk;
    }

    private void add(List<String> idList) {
        chargePointRepository.addChargePointList(idList);
        chargePointHelperService.removeUnknown(idList);
    }

}
