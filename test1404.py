'''Тестирую новые вертикали привлечения с платежами вместо начислений и корповой логике прямо в коде вертикалей. Добавил еще пару изменений. Починил Корпов с начислениями, но без уроков, починил Rocket Cat детей.
'''

from skyeng_airflow import User
from skyeng_airflow.dag_templates import DatasourceBuilderTemplate
from skyeng_airflow.dag_templates.model_builder_validators import UniqueKeyValidator
from skyeng_airflow.hooks.dwh_hooks import Type, DWHHook

tpl = DatasourceBuilderTemplate(
    User.Morozov,
    schedule_interval='@once',
 
    validators=[UniqueKeyValidator(key='order_id')],
    fields={
        'user_id': Type.INTEGER,
        'order_id': Type.INTEGER,
        'education_service_id': Type.INTEGER,
        'vertical': Type.VARCHAR(32),
        'first_paid_balance_date': Type.TIMESTAMP,
        'company_id': Type.INTEGER
    },
    query='''
             WITH fixed_roles AS (
          SELECT DISTINCT
            user_id,
            role_id
          FROM entity.user_role
        ),
         payments as (
            select
            education_service_id as order_id,
            status_set_at,
            ROW_NUMBER() OVER (PARTITION BY education_service_id ORDER BY status_set_at) payment_number
            from datasource.payment_general_with_order
            where 1=1
            and status IN (4,5)
            AND operation_type_id IN (0, 12) --убираем 17ый тип
            AND not is_gift
            and subject in ('mathematics','english') ),
            
         first_payment_status_workflow_interval AS (
          SELECT
             fbc.order_id,
             kba.start_date,
             kba.end_date
          FROM payments fbc
          INNER JOIN datasource.kid_becomes_adult kba
            ON kba.order_id = fbc.order_id
              AND fbc.status_set_at BETWEEN kba.start_date AND kba.end_date
            WHERE payment_number = 1
       ), 
       /*далее код логики корпов с вырезанной информации о менеджерах продаж*/
     all_pays as
(
select *,
    case when operation_type_id in (0,12) then pay_date
        when operation_type_id in (10,17) then first_lesson_after_payment
        else null end as date_res
from (
select stud.order_id,
    pay.payment_id as pay_id,
    pay.operation_type_id,
    pay.status_set_at as pay_date,
    pay.amount,
    pay.currency,
    pay.payment_number as paid_num,
    MIN(case when class.class_id IS NOT NULL THEN class.start_at else null end) as first_lesson_after_payment
from datasource.student_company_log stud
inner join datasource.payment_general_with_order pay
    on stud.order_id = pay.education_service_id
    and ((pay.operation_type_id in (0,12,17) and pay.payment_number > 0)
        or (pay.operation_type_id in (10) and pay.product_item_count > 0))
left join datasource.class_general class
    on stud.order_id = class.education_service_id
    and class.start_at > pay.status_set_at
    and class.type != 'trial'
    and class.status in ('success', 'failed_by_student')
group by stud.order_id,
    pay.payment_id,
    pay.operation_type_id,
    pay.status_set_at,
    pay.amount,
    pay.currency,
    pay.payment_number
)
),

transfer_num as
(
select pay.order_id,
    pay.pay_id,
    pay.operation_type_id,
    case when pay.operation_type_id in (0,12) then pay.amount
        else 0 end as amount,
    pay.currency,
    pay.date_res,
    row_number() over (partition by pay.order_id order by pay.date_res asc) as pay_num, --добавила это условие для трансферов (payment.paid_num пользоваться нельзя)
    sch.company_id,
    sch.start_date as c_start_date,
    sch.end_date as c_end_date,
    dc.pay_type,
  	dc.source_type
    --fda.fact_date_acc as acc_date_fact
from all_pays pay
--left для корректного подсчета номера оплаты
left join datasource.student_company_log sch
   on pay.order_id = sch.order_id
   and pay.date_res >= sch.start_date
   and pay.date_res <= sch.end_date
--добавляю тип компании
left join datasource.company dc
    on sch.company_id = dc.company_id
where (pay.operation_type_id in (0,12,17) or (pay.operation_type_id in (10) and dc.pay_type in (1,2,3))) --исключаю трансферы у спецпредложения
),
-- проверка, попадает ли дата учета У в период компании, добавление МП и аккаунтера
student_manager as
(
select distinct tn.*,
    resp.manager_id,
    resp.manager_role,
    resp.start_date as resp_start_date,
    resp.end_date as resp_end_date,
    op.operator_name as manager_name,
    case when op.group_id in (select group_id from datasource.corp_sale_manager_group) then op.group_id
        else 0 end as group_id,
    case when op.group_id in (select group_id from datasource.corp_sale_manager_group) then op.group_name
        else 'Нет группы' end as group_name
from transfer_num tn
left join datasource.corp_responsible_manager resp
    on tn.company_id = resp.company_id
    and tn.date_res >= resp.start_date
   and tn.date_res <= resp.end_date
left join datasource.operators_corp op
    on resp.manager_id = op.crm_id
    and op.group_id in (select group_id from datasource.corp_sale_manager_group)
where tn.company_id is not null
), corp_logic as
    (select distinct order_id,
                    pay_id,
                    operation_type_id,
                    date_res,
                    source_type,
                    pay_type as comp_type,
                    company_id,
                    case
                        when source_type in ('corporate', 'corp_unknown', 'b2b-telemarketing') and
                             group_id not in (88, 94) then 'corporate'
                        when source_type in ('b2g') then 'b2g'
                        when source_type in ('b2b-international') then 'international'
                        when source_type in ('corporate', 'corp_unknown', 'b2b-telemarketing') and group_id in (88, 94)
                            then 'incorrect_source_type'
                        else 'smth'
                        end  as department
    from student_manager
    WHERE pay_num = 1) /* конец корповой логики */



SELECT
    NVL(crm2.student_id, crm1.user_id) AS user_id,
    NVL(crm2.education_service_id, crm1.order_id) AS order_id,
    CASE
        WHEN crm.crm_type = 1 THEN CASE
                    WHEN corp_logic.company_id = 3817 THEN 'Rocket Cat' --вытащил Rocket Cat выше детской логики
                    WHEN crm1.kid_project = 'math' OR math_role.role_id IS NOT NULL
                        THEN 'Math'
                    WHEN crm1.status_workflow = 2 OR kids_role.role_id IS NOT NULL
                        THEN 'Kids'
                    WHEN crm1.status_workflow = 1 AND fpwl.start_date is not null
                        THEN 'Kids'
                    WHEN corp_logic.department = 'corporate'
                        THEN CASE
                            WHEN corp_logic.comp_type in (1,2,3)
                                THEN 'Corp'
                            WHEN corp_logic.comp_type in (4,5)
                                THEN 'Special Offer'
                            ELSE 'Corp (unknown pay_type)' END
                    WHEN corp_logic.department = 'b2g' THEN 'B2G'
                    WHEN corp_logic.department = 'b2b-international' THEN 'B2B International'
                    WHEN corp_logic.department = 'incorrect_source_type' THEN 'Corp (incorrect_source_type)'
                    ELSE 'Adults' END
        WHEN crm.crm_type = 2
            THEN CASE
                WHEN corp_logic.company_id = 3817 THEN 'Rocket Cat' 
                WHEN crm2.subject = 'mathematics' THEN 'Math'
                WHEN crm2.gradation = 'junior' OR crm2.gradation = 'klp' THEN 'Kids'
                WHEN corp_logic.department = 'corporate'
                        THEN CASE
                            WHEN corp_logic.comp_type in (1,2,3)
                                THEN 'Corp'
                            WHEN corp_logic.comp_type in (4,5)
                                THEN 'Special Offer'
                            ELSE 'Corp (unknown pay_type)' END
                    WHEN corp_logic.department = 'b2g' THEN 'B2G'
                    WHEN corp_logic.department = 'b2b-international' THEN 'B2B International'
                    WHEN corp_logic.department = 'incorrect_source_type' THEN 'Corp (incorrect_source_type)'
                ELSE 'Adults' END
        WHEN crm.crm_type IS NULL
            THEN CASE
                    WHEN corp_logic.company_id = 3817 THEN 'Rocket Cat' 
                    WHEN corp_logic.department = 'corporate'
                        THEN CASE
                            WHEN corp_logic.comp_type in (1,2,3)
                                THEN 'Corp'
                            WHEN corp_logic.comp_type in (4,5)
                                THEN 'Special Offer'
                            ELSE 'Corp (unknown pay_type)' END
                    WHEN corp_logic.department = 'b2g' THEN 'B2G'
                    WHEN corp_logic.department = 'b2b-international' THEN 'B2B International'
                    WHEN corp_logic.department = 'incorrect_source_type' THEN 'Corp (incorrect_source_type)'
                ELSE NULL END
        ELSE NULL
        END AS vertical,
        pay.status_set_at AS first_paid_balance_date,
       corp_logic.company_id AS company_id

FROM entity."order" crm1
FULL JOIN entity.order_crm2 crm2
    ON crm1.order_id = crm2.education_service_id
LEFT JOIN payments pay
    ON pay.order_id = NVL(crm2.education_service_id, crm1.order_id)
    AND  pay.payment_number = 1
LEFT JOIN datasource.student_crm_log crm
            ON crm.order_id = NVL(crm2.education_service_id, crm1.order_id)
            AND pay.status_set_at BETWEEN crm.start_date AND crm.end_date
LEFT JOIN corp_logic ON  corp_logic.order_id = NVL(crm2.education_service_id, crm1.order_id)
LEFT JOIN fixed_roles AS math_role
            ON  crm1.user_id = math_role.user_id AND math_role.role_id = 978 -- ROLE_MATH_STUDENT
LEFT JOIN fixed_roles AS kids_role
            ON crm1.user_id = kids_role.user_id AND kids_role.role_id = 195
LEFT JOIN first_payment_status_workflow_interval fpwl
            ON crm1.user_id = fpwl.order_id
    '''
)

dag = tpl.DAG()
