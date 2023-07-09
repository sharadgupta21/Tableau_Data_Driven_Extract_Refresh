-- you can use this below sql query to create a tableau workbook to find tableau object LUID using ID or Object Name. You can use tableau repo readonly user and tableau repo for data source connection.

SELECT 
  'Published Datasource' object_type,
  ds.id object_id,
  ds.name object_name,
  CAST(ds.luid AS TEXT) as object_luid,
  ds.project_id,
  prj.name project_name,
  ds.owner_id object_owner_id,
  usr.friendly_name object_owner_name,
  usr.name as object_owner_email
FROM public.datasources ds
inner join public.projects prj on ds.project_id = prj.id
inner join public._users usr on ds.owner_id = usr.id
where ds.site_id = 3

union all

SELECT 
  'Workbook' object_type,
  wb.id object_id,
  wb.name object_name,
  CAST(wb.luid AS TEXT) as object_luid,
  wb.project_id,
  prj.name project_name,
  wb.owner_id object_owner_id,
  usr.friendly_name object_owner_name,
  usr.name as object_owner_email
FROM public.workbooks wb
inner join public.projects prj on wb.project_id = prj.id
inner join public._users usr on wb.owner_id = usr.id
where wb.site_id = 3