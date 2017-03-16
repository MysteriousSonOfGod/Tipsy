import discord, pymysql.cursors
from discord.ext import commands
from utils import rpc_module, mysql_module, parsing

#result_set = database response with parameters from query
#db_bal = nomenclature for result_set["balance"]
#snowflake = snowflake from message context, identical to user in database
#wallet_bal = nomenclature for wallet reponse

rpc = rpc_module.Rpc()
Mysql = mysql_module.Mysql()

class Balance:

    def __init__(self, bot):
        self.bot = bot
        self.config = parsing.parse_json("./config.json")
        self.config_mysql = self.config["mysql"]
        self.host = self.config_mysql["db_host"]
        try:
            self.port = int(self.config_mysql["db_port"])
        except KeyError:
            self.port = 3306

    async def do_embed(self, name, db_bal, db_staked, db_unconf):
        # Simple embed function for displaying username and balance
        embed = discord.Embed(colour=name.top_role.colour)
        embed.add_field(name="User", value=name.mention)
        embed.add_field(name="Balance", value="%.8f" % round(float(db_bal),8))
        embed.add_field(name="Staked", value="%.8f" % round(float(db_staked), 8))
        embed.add_field(name="Unconfirmed", value="%.8f" % round(float(db_unconf), 8))
        embed.set_footer(text="Sponsored by altcointrain.com - Choo!!! Choo!!!")

        try:
            await self.bot.say(embed=embed)
        except discord.HTTPException:
            await self.bot.say("I need the `Embed links` permission to send this")

    @commands.command(pass_context=True)
    async def balance(self, ctx):
        """Display your balance"""
        # Set important variables
        snowflake = ctx.message.author.id
        name = ctx.message.author

        # Check if user exists in db
        result_set = Mysql.check_for_user(name, snowflake)
        
        db_bal, db_staked, db_unconf = Mysql.get_unconf_and_balance(name, snowflake)
        await self.do_embed(name, db_bal, db_staked, db_unconf)


def setup(bot):
    bot.add_cog(Balance(bot))
