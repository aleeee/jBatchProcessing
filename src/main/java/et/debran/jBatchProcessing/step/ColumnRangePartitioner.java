package et.debran.jBatchProcessing.step;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class ColumnRangePartitioner implements Partitioner{
	
	private NamedParameterJdbcTemplate jdbcTemplate;
	private String tableName;
	private String column;
	private String whereCondition;
	private Map<String, Object> parameters;
	
		public ColumnRangePartitioner(NamedParameterJdbcTemplate jdbcTemplate, String tableName, String column,
			String whereCondition, Map<String, Object> parameters) {
		super();
		this.jdbcTemplate = jdbcTemplate;
		this.tableName = tableName;
		this.column = column;
		this.whereCondition = whereCondition;
		this.parameters = parameters;
	}

		@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
			int minIndex = jdbcTemplate.queryForObject("select min(" +column + ") as id from " + tableName + " "+ whereCondition, parameters, 
				 new RowMapper<Integer>() {

					@Override
					public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
						try {
						int min = rs.getInt("id");
						return min;
						}catch(Exception e){
							return 0;
						}
					}
				});
//		int minIndex = jdbcTemplate.queryForObject("select min(" +column + ") from " + tableName + " "+ whereCondition, parameters, Integer.class);
//		int maxIndex = jdbcTemplate.queryForObject("select max(" +column + ") from " + tableName + " "+ whereCondition, parameters, Integer.class);
		int maxIndex = jdbcTemplate.queryForObject("select max(" +column + ") as id from " + tableName + " "+ whereCondition, parameters, 
				new RowMapper<Integer>() {

			@Override
			public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
				try {
				int max = rs.getInt("id");
				return max;
				}catch(Exception e){
					return 0;
				}
			}
		});
		
		int partitionSize = (maxIndex - minIndex) /gridSize +1;
		
		Map<String, ExecutionContext> contexts = new HashMap<>();
		int counter =0;
		int start=minIndex;
		int end= start + partitionSize -1;
		
		while(start <= maxIndex) {
			ExecutionContext ctx = new ExecutionContext();
			contexts.put("partition"+counter, ctx);
			
			if(end >= maxIndex) {
				end = maxIndex;
			}
			
			ctx.put("minIndex", start);
			ctx.put("maxIndex", end);
			
			start += partitionSize;
			end += partitionSize;
			counter++;
		}
		return contexts;
		
		
	}

}
