import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Reducer
{
	public static void main (String arg[])
	{
		try
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String input;
			while((input = br.readLine()) != null)
			{
				String[] strs = input.split("\t");
				if(strs.length == 3)
				{
					String uid = "userid" + (char)3 + "{\"n\":\"" + strs[0] + "\"}";
					String tim = "time" + (char)3 + "{\"s\":\"" + strs[1] + "\"}";
					String pic = "url" + (char)3 + "{\"s\":\"" + strs[2] + "\"}";
					System.out.println(pic + (char)2 + uid + (char)2 + tim);
				}
			}
			br.close();
		}
		catch(IOException io)
		{
			io.printStackTrace();
		}
	}
}	
