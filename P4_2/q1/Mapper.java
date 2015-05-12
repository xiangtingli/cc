import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Mapper
{
	public static void main (String arg[])
	{
		try
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String input;
			while((input = br.readLine()) != null)
			{
				int pos1 = input.indexOf('@');
				int pos2 = input.lastIndexOf(',');
				if(pos1 < 0 || pos2 < 0)
				{
					continue;
				}
				String title = input.substring(pos1 + 1, pos2);
				String freq = input.substring(pos2 + 1, input.length() - 1);
				System.out.println(freq + "\t" + title);
			}
			br.close();
		}
		catch(IOException io)
		{
			io.printStackTrace();
		}
	}
}	
