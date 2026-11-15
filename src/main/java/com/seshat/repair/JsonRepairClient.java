package com.seshat.repair;

public interface JsonRepairClient {

    Profile profile();

    // HTTP 연결부는 요청의 제한 시간 안에 완료하고 실패를 공통 오류로 변환한다.
    RepairCandidate repair(RepairRequest request) throws RepairCallException;

    // 대상 ID는 호출 서버가 바뀌면 달라져야 하며 인증 정보는 포함하지 않는다.
    record Profile(String targetId, String model, String promptVersion) {
    }
}
