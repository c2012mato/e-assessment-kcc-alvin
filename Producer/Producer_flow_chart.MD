```mermaid
flowchart TD
    Start([Start: HTTP Request])
    MethodCheck{Request Method == POST?}
    APIKeyCheck{API Key valid?}
    ParsePayload{Payload parse OK & action present?}
    ActionCheck{action == Start?}
    DurationCheck{Duration valid & <= 20?}
    InitTimes[Set start_time, end_time]
    EventLoop{now < end_time?}
    GenerateEvent[Generate event_id, user_id, timestamps]
    RandomSleep{10% chance: Sleep 30-60s}
    PublishEvent[Publish event to PubSub]
    CorrectionCheck{5-10% chance: Resend as invalid}
    CorrectionDelay[Sleep 1-5s]
    PublishCorrection[Publish correction event]
    Wait1s[Sleep 1s]
    Response([Return Success JSON])
    Error405([405 Method Not Allowed])
    Error401([401 Unauthorized])
    Error400Action([400 Missing action])
    Error400Duration([400 Missing/invalid duration])
    Error400Invalid([400 Invalid action])

    %% Flow
    Start --> MethodCheck
    MethodCheck -- No --> Error405
    MethodCheck -- Yes --> APIKeyCheck
    APIKeyCheck -- No --> Error401
    APIKeyCheck -- Yes --> ParsePayload
    ParsePayload -- No --> Error400Action
    ParsePayload -- Yes --> ActionCheck
    ActionCheck -- No --> Error400Invalid
    ActionCheck -- Yes --> DurationCheck
    DurationCheck -- No --> Error400Duration
    DurationCheck -- Yes --> InitTimes
    InitTimes --> EventLoop

    EventLoop -- No --> Response
    EventLoop -- Yes --> GenerateEvent
    GenerateEvent --> RandomSleep
    RandomSleep --> PublishEvent
    PublishEvent --> CorrectionCheck
    CorrectionCheck -- No --> Wait1s
    CorrectionCheck -- Yes --> CorrectionDelay
    CorrectionDelay --> PublishCorrection
    PublishCorrection --> Wait1s
    Wait1s --> EventLoop
