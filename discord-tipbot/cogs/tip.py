import discord, json, requests, pymysql.cursors
from discord.ext import commands
from utils import rpc_module, mysql_module

rpc = rpc_module.Rpc()
Mysql = mysql_module.Mysql()


class Tip:
    def __init__(self, bot):
        self.bot = bot

    @commands.command(pass_context=True)
    async def tip(self, ctx, user:discord.Member, amount:float):
        """Tip a user coins"""
        snowflake = ctx.message.author.id
        name = ctx.message.author

        tip_user = user.id
        if snowflake == tip_user:
            await self.bot.say("{} **:warning:You cannot tip yourself!:warning:**".format(name.mention))
            return

        if amount <= 0.0000000:
            await self.bot.say("{} **:warning:You cannot tip zero!:warning:**".format(name.mention))
            return

        Mysql.check_for_user(name, snowflake)

        result_set = Mysql.get_bal_lasttxid(snowflake)

        if float(result_set["balance"]) < amount:
            await self.bot.say("{} **:warning:You cannot tip more money than you have!:warning:**".format(name.mention))
        else:
            tip_user_addy = rpc.getaccountaddress(tip_user)

            rpc.sendfrom(snowflake, tip_user_addy, amount)
            await self.bot.say("{} **tipped {} {} NET! :money_with_wings:**".format(name.mention, user.mention, str(amount)))

def setup(bot):
    bot.add_cog(Tip(bot))
