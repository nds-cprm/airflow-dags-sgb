-- Analisar colunas de geometria
SELECT DISTINCT 
	REPLACE(sde.st_geometrytype(shape), 'ST_', '') AS geometry_type,
	CASE sde.st_entity(shape)
		WHEN 0 THEN 'nil shape'
		WHEN 1 THEN 'point'
		WHEN 2 THEN 'line (includes spaghetti lines)'
		WHEN 4 THEN  'linestring'
		WHEN 8 THEN 'area'
		WHEN 257 THEN 'multipoint'
		WHEN 258 THEN 'multiline (includes spaghetti lines)'
		WHEN 260 THEN 'multilinestring'
		WHEN 264 THEN 'multiarea'
		ELSE 'unknown' 
	END AS entity,
	sde.st_is3d(shape) AS is_3d,
	sde.st_ismeasured(shape) AS is_measured, -- IS measured (M)
	sde.st_isempty(shape) AS is_empty,
	sde.ST_IsSimple(shape) AS is_simple -- IS OGC Simple
FROM LITOESTRATIGRAFIA.UE_LAYER_100000;