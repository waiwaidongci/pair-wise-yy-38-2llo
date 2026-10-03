from __future__ import annotations
from .domain import ConflictError, ValidationError
TITLE='水库防汛调度与操作确认'; ENTITY='调度指令'; OBSERVATION_ENTITY='水情观测'; ID_PREFIX='RF'
SEVERITIES=['routine', 'attention', 'urgent', 'emergency']
# 标准生命周期状态（线性，供常规调度与完整流程使用）
STATES=['draft', 'checked', 'authorized', 'executed', 'closed']
# supplement_pending：历史指令缺依据版本，待补核；recheck_pending：观测更新后旧复核失效，退回待复核
SUP_STATE='supplement_pending'; RECHECK_STATE='recheck_pending'
ALL_STATES=STATES+[SUP_STATE, RECHECK_STATE]
# 常规生命周期流转；两个挂起态只能通过专门的补核/重新复核动作离开
TRANSITIONS={'draft': ['checked'], 'checked': ['authorized'], 'authorized': ['executed'], 'executed': ['closed'], 'closed': [], SUP_STATE: [], RECHECK_STATE: []}
TRANSITION_ROLES={'checked': ['duty_officer'], 'authorized': ['chief_engineer'], 'executed': ['dispatcher'], 'closed': ['chief_engineer']}
CREATE_ROLES=set(['duty_officer']); RECORD_ROLES=set(['duty_officer', 'dispatcher']); AUDIT_ROLES=set(['chief_engineer', 'viewer']); VIEW_ROLES=set(['duty_officer', 'chief_engineer', 'dispatcher', 'viewer'])
OBSERVATION_ROLES=set(['duty_officer', 'chief_engineer']); SUPPLEMENT_ROLES=set(['duty_officer', 'chief_engineer']); RECHECK_ROLES=set(['duty_officer'])
SEVERITY_WEIGHT={'routine': 1.0, 'attention': 3.0, 'urgent': 6.0, 'emergency': 9.0}; DEADLINE_HOURS={'routine': 72, 'attention': 24, 'urgent': 8, 'emergency': 4}; TERMINAL_STATES=set(['closed'])
# 观测更新后需要作废复核、退回待复核的状态（未执行指令）
INVALIDATABLE_STATES=set(['checked', 'authorized'])
def priority_score(severity,quantity=0.0,threshold=1.0,open_records=0):
    if severity not in SEVERITY_WEIGHT: raise ValidationError("unknown severity")
    ratio=quantity/threshold if threshold>0 else 1.0
    return max(0,min(10,int(round(SEVERITY_WEIGHT[severity]+min(4.0,ratio*4.0)+min(3.0,float(open_records))))))
def response_deadline_hours(severity,quantity=0.0,threshold=1.0):
    if severity not in DEADLINE_HOURS: raise ValidationError("unknown severity")
    ratio=quantity/threshold if threshold>0 else 1.0
    return max(1,int(DEADLINE_HOURS[severity]/max(1.0,ratio)))
def escalation_required(severity,quantity=0.0,threshold=1.0):
    return severity==SEVERITIES[-1] or (threshold>0 and quantity>=threshold)
def can_transition(current,target): return target in TRANSITIONS.get(current,[])
def validate_transition(current,target):
    if current not in ALL_STATES or target not in ALL_STATES: raise ValidationError("未知状态")
    if not can_transition(current,target):
        if current==SUP_STATE: raise ConflictError("指令待补核，请先补充依据版本")
        if current==RECHECK_STATE: raise ConflictError("依据已更新，旧复核失效，请重新复核")
        raise ConflictError(f"不能从{current}转换到{target}")
def completion_blockers(target,open_records): return ["仍有未关闭事项"] if target in TERMINAL_STATES and open_records>0 else []
def role_for_transition(target): return set(TRANSITION_ROLES.get(target,[]))
